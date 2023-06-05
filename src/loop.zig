const std = @import("std");
const os = std.os;
const assert = std.debug.assert;

const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

const FIFO = @import("fifo.zig").FIFO;
const Completion = @import("completion.zig").Completion;

pub const Loop = struct {
    const InitOptions = struct {
        entries: u13 = 256,
    };

    ring: IO_Uring,
    /// Number of completions submitted to kernel, or waiting to be submitted
    /// in the submissions queue.
    active: usize = 0,
    /// Number of completions submitted to the kernel.
    in_kernel: usize = 0,
    /// Completions not yet submitted to the kernel and waiting on available
    /// space in the submission queue.
    submissions: FIFO(Completion) = .{},

    pub fn init(opt: InitOptions) !Loop {
        return .{
            .ring = try IO_Uring.init(opt.entries, 0),
        };
    }

    pub fn deinit(self: *Loop) void {
        self.ring.deinit();
    }

    /// Put completion into submission queue. If submission queue is full store
    /// it into submissions fifo.
    pub fn submit(self: *Loop, completion: *Completion) void {
        assert(completion.ready());
        completion.sumitted();
        self.active += 1;
        // try to get place in ring submission queue
        const sqe = self.ring.get_sqe() catch |err| {
            // if queue is full put it into submissions
            assert(err == error.SubmissionQueueFull);
            self.submissions.push(completion);
            return;
        };
        completion.prep(sqe);
    }

    pub const RunMode = enum {
        no_wait,
        once,
        until_done,
    };

    pub fn run(self: *Loop, mode: RunMode) !void {
        const wait_nr: u32 = switch (mode) {
            .until_done => 1,
            .once => 1,
            .no_wait => 0,
        };

        while (true) {
            if (self.active == 0) break;

            try self.prep_submissions();
            self.in_kernel += try self.ring.submit_and_wait(wait_nr);
            const completed = try self.flush_completions(0);

            switch (mode) {
                .no_wait => break,
                .once => if (completed > 0) break,
                .until_done => {},
            }
        }
    }

    fn prep_submissions(self: *Loop) !void {
        while (self.submissions.peek()) |completion| {
            const sqe = self.ring.get_sqe() catch |err| {
                assert(err == error.SubmissionQueueFull);
                return;
            };
            completion.prep(sqe);
            _ = self.submissions.pop();
        }
    }

    fn flush_completions(self: *Loop, wait_nr: u32) !u32 {
        var cqes: [256]io_uring_cqe = undefined;
        var no_completed: u32 = 0;
        while (true) {
            // read completed from completion queue
            const len = self.ring.copy_cqes(&cqes, wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            self.in_kernel -= len;
            self.active -= len;
            no_completed += len;
            for (cqes[0..len]) |cqe| {
                // call completion callback
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                completion.completed(cqe.err(), cqe.res, cqe.flags);
            }
            if (len < cqes.len) return no_completed;
        }
    }
};
