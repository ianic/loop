const std = @import("std");
const os = std.os;
const net = std.net;
const mem = std.mem;
const Allocator = mem.Allocator;

const assert = std.debug.assert;
const testing = std.testing;

const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

const errno = @import("errno.zig");

test "Completion complete" {
    // define Context
    const Context = struct {
        calls: usize = 0,

        const Context = @This();

        fn acceptSuccess(self: *@This(), socket: os.socket_t) CompleteAction {
            assert(socket == 123);
            self.calls += 1;
            return .disarm;
        }

        fn acceptFailure(self: *Context, _: anyerror, _: ?os.E) void {
            _ = self;
        }
    };
    var ctx = Context{};

    var completion = Completion.accept(&ctx, Context.acceptSuccess, Context.acceptFailure, 0);

    // complete completion, expect to call acceptCallback
    _ = completion.complete(&completion, 123);
    try testing.expectEqual(@as(usize, 1), ctx.calls);
}

test "Completion align" {
    try testing.expectEqual(8, @alignOf(Completion));
    try testing.expectEqual(144, @sizeOf(Completion));
}

pub const Error = error{
    //Canceled,
    OutOfMemory,
} || errno.Error;

const CompleteAction = enum {
    disarm,
    rearm,
};

const Completion = struct {
    operation: Operation,
    context: ?*anyopaque,
    complete: *const fn (completion: *Completion, result: i32) CompleteAction,
    fail: *const fn (completion: *Completion, err: anyerror) void,

    fn prep(completion: *Completion, sqe: *io_uring_sqe) void {
        switch (completion.operation) {
            .accept => |*op| {
                linux.io_uring_prep_accept(sqe, op.socket, &op.address, &op.address_size, os.SOCK.CLOEXEC);
            },
            .close => |op| {
                linux.io_uring_prep_close(sqe, op.fd);
            },
            .connect => |*op| {
                linux.io_uring_prep_connect(sqe, op.socket, &op.address.any, op.address.getOsSockLen());
            },
            .read => |op| {
                linux.io_uring_prep_read(sqe, op.fd, op.buffer[0..op.buffer.len], op.offset);
            },
            .recv => |op| {
                linux.io_uring_prep_recv(sqe, op.socket, op.buffer, os.MSG.NOSIGNAL);
            },
            .send => |op| {
                linux.io_uring_prep_send(sqe, op.socket, op.buffer, os.MSG.NOSIGNAL);
            },
            .timeout => |*op| {
                linux.io_uring_prep_timeout(sqe, &op.timespec, 0, 0);
            },
            .write => |op| {
                linux.io_uring_prep_write(sqe, op.fd, op.buffer[0..op.buffer.len], op.offset);
            },
        }
        sqe.user_data = @ptrToInt(completion);
    }

    fn accept(
        context: anytype,
        comptime success: fn (context: @TypeOf(context), socket: os.socket_t) CompleteAction,
        comptime failure: fn (context: @TypeOf(context), err: anyerror, errno: ?os.E) void,
        socket: os.socket_t,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(comp: *Completion, result: i32) CompleteAction {
                var ctx = @intToPtr(Context, @ptrToInt(comp.context));
                if (result < 0) {
                    const ose: os.E = @intToEnum(os.E, result);
                    if (ose == .INTR) {
                        return .rearm;
                    }
                    failure(ctx, errno.toError(ose), ose);
                    return .disarm;
                }
                return success(ctx, @intCast(os.socket_t, result));
            }
            fn fail(comp: *Completion, err: anyerror) void {
                var ctx = @intToPtr(Context, @ptrToInt(comp.context));
                failure(ctx, err, null);
            }
        };
        return .{
            .operation = .{ .accept = .{ .socket = socket } },
            .context = context,
            .complete = wrapper.complete,
            .fail = wrapper.fail,
        };
    }
};

/// This union encodes the set of operations supported as well as their arguments.
const Operation = union(enum) {
    accept: struct {
        socket: os.socket_t,
        address: os.sockaddr = undefined,
        address_size: os.socklen_t = @sizeOf(os.sockaddr),
    },
    close: struct {
        fd: os.fd_t,
    },
    connect: struct {
        socket: os.socket_t,
        address: net.Address,
    },
    read: struct {
        fd: os.fd_t,
        buffer: []u8,
        offset: u64,
    },
    recv: struct {
        socket: os.socket_t,
        buffer: []u8,
    },
    send: struct {
        socket: os.socket_t,
        buffer: []const u8,
    },
    timeout: struct {
        timespec: os.linux.kernel_timespec,
    },
    write: struct {
        fd: os.fd_t,
        buffer: []const u8,
        offset: u64,
    },
};

const CallbackAction = struct {
    loop: *Loop,
    completion: *Completion,

    pub fn rearm(self: *CallbackAction) !void {
        try self.loop.enqueue(self.completion);
    }
};

pub const Loop = struct {
    const CompletionPool = std.heap.MemoryPool(Completion);
    const InitOptions = struct {
        entries: u13 = 256,
    };
    const Fifo = std.fifo.LinearFifo(*Completion, .Dynamic);

    ring: IO_Uring,
    completion_pool: CompletionPool,
    /// Number of completions submitted to kernel, or waiting to be submitted
    /// in the unqueued fifo.
    active: usize = 0,
    /// Number of completions submitted to the kernel.
    in_kernel: usize = 0,
    /// Operations not yet submitted to the kernel and waiting on available
    /// space in the submission queue.
    unqueued: Fifo,

    fn init(alloc: Allocator, opt: InitOptions) !Loop {
        return .{
            .ring = try IO_Uring.init(opt.entries, 0),
            .completion_pool = CompletionPool.init(alloc),
            .unqueued = Fifo.init(alloc),
        };
    }

    pub fn deinit(self: *Loop) void {
        self.unqueued.deinit();
        self.completion_pool.deinit();
        self.ring.deinit();
    }

    pub fn accept(
        self: *Loop,
        context: anytype,
        comptime success: fn (context: @TypeOf(context), socket: os.socket_t) CompleteAction,
        comptime failure: fn (context: @TypeOf(context), err: anyerror, errno: ?os.E) void,
        socket: os.socket_t,
    ) void {
        self.accept_(context, success, failure, socket) catch |err| {
            failure(context, err, null);
        };
    }

    fn accept_(
        self: *Loop,
        context: anytype,
        comptime success: fn (context: @TypeOf(context), socket: os.socket_t) CompleteAction,
        comptime failure: fn (context: @TypeOf(context), err: anyerror, errno: ?os.E) void,
        socket: os.socket_t,
    ) !void {
        var completion = try self.completion_pool.create(); // get completion from the pool
        errdefer self.completion_pool.destroy(completion); // return to the pool
        completion.* = Completion.accept(context, success, failure, socket); // fill with accept information
        try self.enqueue(completion);
    }

    /// Put completion into submission queue. If submission queue is full store
    /// it into unqueued fifo.
    fn enqueue(self: *Loop, completion: *Completion) !void {
        const sqe = self.ring.get_sqe() catch |err| {
            assert(err == error.SubmissionQueueFull);
            try self.unqueued.writeItem(completion);
            self.active += 1;
            return;
        };
        completion.prep(sqe);
        self.active += 1;
    }

    const RunMode = enum {
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

            try self.prep_unqueued();
            self.in_kernel += try self.ring.submit_and_wait(wait_nr);
            const completed = try self.flush_completions(0);

            switch (mode) {
                .no_wait => break,
                .once => if (completed > 0) break,
                .until_done => {},
            }
        }
    }

    fn prep_unqueued(self: *Loop) !void {
        while (self.unqueued.count > 0) {
            const completion = self.unqueued.peekItem(0);
            const sqe = self.ring.get_sqe() catch |err| {
                assert(err == error.SubmissionQueueFull);
                return;
            };
            completion.prep(sqe);
            self.unqueued.discard(1);
        }
    }

    fn flush_completions(self: *Loop, wait_nr: u32) !u32 {
        var cqes: [256]io_uring_cqe = undefined;
        while (true) {
            // read completed from completion queue
            const completed = self.ring.copy_cqes(&cqes, wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            self.in_kernel -= completed;
            self.active -= completed;
            for (cqes[0..completed]) |cqe| {
                // call completion callback
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                switch (completion.complete(completion, cqe.res)) {
                    .disarm => {
                        self.completion_pool.destroy(completion);
                    },
                    .rearm => {
                        self.enqueue(completion) catch |err| {
                            completion.fail(completion, err);
                            self.completion_pool.destroy(completion);
                        };
                    },
                }
            }
            if (completed < cqes.len) return completed;
        }
    }
};

pub fn listen(address: net.Address) !os.socket_t {
    const kernel_backlog = 1;
    const listener_socket = try os.socket(address.any.family, os.SOCK.STREAM | os.SOCK.CLOEXEC, 0);
    errdefer os.closeSocket(listener_socket);

    try os.setsockopt(listener_socket, os.SOL.SOCKET, os.SO.REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try os.bind(listener_socket, &address.any, address.getOsSockLen());
    try os.listen(listener_socket, kernel_backlog);
    return listener_socket;
}

test "accept" {
    const address = try net.Address.parseIp4("127.0.0.1", 3131);
    const listener_socket = try listen(address);
    var loop = try Loop.init(testing.allocator, .{});
    defer loop.deinit();

    const Context = struct {
        const Self = @This();
        calls: usize = 0,

        fn acceptSuccess(self: *Self, socket: os.socket_t) CompleteAction {
            _ = socket;
            self.calls += 1;
            return .disarm;
        }

        fn acceptFailure(self: *Self, _: anyerror, _: ?os.E) void {
            _ = self;
        }
    };
    var ctx = Context{};

    try testing.expectEqual(@as(usize, 0), loop.active);
    loop.accept(&ctx, Context.acceptSuccess, Context.acceptFailure, listener_socket);
    try testing.expectEqual(@as(usize, 1), loop.active);
    const thr = try std.Thread.spawn(.{}, testConnect, .{address});
    try loop.run(.once);
    try testing.expectEqual(@as(usize, 0), loop.active);
    try testing.expectEqual(@as(usize, 1), ctx.calls);
    thr.join();
}

fn testConnect(address: net.Address) !void {
    var conn = try std.net.tcpConnectToAddress(address);
    conn.close();
}
