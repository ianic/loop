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

        fn acceptCallback(self: *@This(), result: Error!os.socket_t) void {
            const res = result catch unreachable;
            assert(res == 123);
            self.calls += 1;
        }
    };
    var ctx = Context{};

    // create Completion
    var completion: Completion = undefined;
    Completion.accept(&completion, undefined, &ctx, Context.acceptCallback, 0);
    completion.result = 123;

    // complete completion, expect to call acceptCallback
    completion.complete();
    try testing.expectEqual(@as(usize, 1), ctx.calls);
}

test "Completion align" {
    try testing.expectEqual(8, @alignOf(Completion));
    try testing.expectEqual(152, @sizeOf(Completion));
}

pub const Error = error{
    //Canceled,
    OutOfMemory,
} || errno.Error;

const Completion = struct {
    const Callback = *const fn (completion: *Completion) void;

    operation: Operation,
    result: i32 = undefined,
    context: ?*anyopaque,
    callback: Callback,
    loop: *Loop,

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

    fn complete(completion: *Completion) void {
        completion.callback(completion);
    }

    fn accept(
        completion: *Completion,
        loop: *Loop,
        context: anytype,
        comptime callback: fn (
            context: @TypeOf(context),
            result: Error!os.socket_t,
        ) void,
        socket: os.socket_t,
    ) void {
        const Context = @TypeOf(context);
        completion.* = .{
            .operation = .{ .accept = .{ .socket = socket } },
            .context = context,
            .loop = loop,
            .callback = (struct {
                fn wrapper(comp: *Completion) void {
                    const result = if (comp.result < 0)
                        errno.toError(@intToEnum(os.E, -comp.result))
                    else
                        @intCast(os.socket_t, comp.result);

                    if (result == Error.InterruptedSystemCall) {
                        comp.loop.retry(comp);
                        return;
                    }

                    callback(
                        @intToPtr(Context, @ptrToInt(comp.context)),
                        @intToPtr(*const Error!os.socket_t, @ptrToInt(&result)).*,
                    );
                }
            }).wrapper,
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

    fn retry(self: *Loop, completion: *Completion) void {
        _ = self;
        _ = completion;
    }

    pub fn accept(
        self: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!os.socket_t) void,
        socket: os.socket_t,
    ) void {
        self.accept_(context, callback, socket) catch |err| {
            callback(context, err);
        };
    }

    fn accept_(
        self: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!os.socket_t) void,
        socket: os.socket_t,
    ) !void {
        var completion = try self.completion_pool.create(); // get completion from the pool
        errdefer self.completion_pool.destroy(completion); // return to the pool
        completion.accept(self, context, callback, socket); // fill with accept information
        try self.enqueue(completion);
    }

    /// Put completion into submission queue. If submission queue is full store
    /// it into unqueued fifo.
    fn enqueue(self: *Loop, completion: *Completion) !void {
        const sqe = self.ring.get_sqe() catch |err| {
            assert(err == error.SubmissionQueueFull);
            self.active += 1;
            try self.unqueued.writeItem(completion);
            return;
        };
        self.active += 1;
        completion.prep(sqe);
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
                completion.result = cqe.res;
                completion.complete();
                // TODO: destroy completion or rearm
                //
                self.completion_pool.destroy(completion); // return to the pool
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

        fn acceptCompleted(self: *Self, result: Error!os.socket_t) void {
            _ = result catch unreachable;
            self.calls += 1;
        }
    };
    var ctx = Context{};

    try testing.expectEqual(@as(usize, 0), loop.active);
    loop.accept(&ctx, Context.acceptCompleted, listener_socket);
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
