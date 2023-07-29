const std = @import("std");
const os = std.os;
const net = std.net;

const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

const errno = @import("errno.zig");

pub const Error = error{
    EOF,
} || errno.Error;

pub const Completion = struct {
    /// This union encodes the set of operations supported as well as their arguments.
    const Args = union(enum) {
        accept: struct {
            socket: os.socket_t,
            address_size: os.socklen_t = @sizeOf(os.sockaddr),
            address: os.sockaddr = undefined,
        },
        close: struct {
            fd: os.fd_t,
        },
        connect: struct {
            socket: os.socket_t,
            address_size: os.socklen_t,
            address: os.sockaddr,
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
        shutdown: struct {
            socket: os.socket_t,
            how: os.ShutdownHow,
        },
    };

    const State = enum {
        initial,
        active, // submitted to the loop
        completed, // returned from cqe completed/failed
    };

    next: ?*Completion = null, // used in fifo
    args: Args,
    state: State = .initial,
    context: ?*anyopaque,
    complete: *const fn (completion: *Completion, ose: os.E, res: i32, flags: u32) void,

    // Ready to be sumitted to the loop
    pub fn ready(self: *Completion) bool {
        return self.state != .active;
    }

    pub fn prep(self: *Completion, sqe: *io_uring_sqe) void {
        switch (self.args) {
            .accept => |*args| {
                linux.io_uring_prep_accept(sqe, args.socket, &args.address, &args.address_size, os.SOCK.CLOEXEC);
            },
            .close => |args| {
                linux.io_uring_prep_close(sqe, args.fd);
            },
            .connect => |*args| {
                linux.io_uring_prep_connect(sqe, args.socket, &args.address, args.address_size);
            },
            .read => |args| {
                linux.io_uring_prep_read(sqe, args.fd, args.buffer, args.offset);
            },
            .recv => |args| {
                linux.io_uring_prep_recv(sqe, args.socket, args.buffer, os.MSG.NOSIGNAL);
            },
            .send => |args| {
                linux.io_uring_prep_send(sqe, args.socket, args.buffer, os.MSG.NOSIGNAL);
            },
            .timeout => |*args| {
                linux.io_uring_prep_timeout(sqe, &args.timespec, 0, 0);
            },
            .write => |args| {
                linux.io_uring_prep_write(sqe, args.fd, args.buffer, args.offset);
            },
            .shutdown => |args| {
                linux.io_uring_prep_shutdown(sqe, args.socket, @intFromEnum(args.how));
            },
        }
        sqe.user_data = @intFromPtr(self);
    }

    pub fn completed(self: *Completion, ose: os.E, res: i32, flags: u32) void {
        self.state = .completed;
        self.complete(self, ose, res, flags);
    }

    pub fn sumitted(self: *Completion) void {
        self.state = .active;
    }

    pub fn close(
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!void) void,
        socket: os.socket_t,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                _ = res;
                var ctx: Context = @ptrFromInt(@intFromPtr(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    callback(ctx, e);
                } else {
                    callback(ctx, {});
                }
            }
        };
        return .{
            .args = .{ .close = .{ .fd = socket } },
            .context = context,
            .complete = wrapper.complete,
        };
    }

    pub fn accept(
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), socket: Error!os.socket_t) void,
        socket: os.socket_t,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx: Context = @ptrFromInt(@intFromPtr(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    callback(ctx, e);
                } else {
                    callback(ctx, @as(os.socket_t, @intCast(res)));
                }
            }
        };
        return .{
            .args = .{ .accept = .{ .socket = socket } },
            .context = context,
            .complete = wrapper.complete,
        };
    }

    pub fn connect(
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!os.socket_t) void,
        socket: os.socket_t,
        address: net.Address,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                _ = res;
                var ctx: Context = @ptrFromInt(@intFromPtr(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    callback(ctx, e);
                } else {
                    callback(ctx, completion.args.connect.socket);
                }
            }
        };
        return .{
            .args = .{ .connect = .{
                .socket = socket,
                .address = address.any,
                .address_size = address.getOsSockLen(),
            } },
            .context = context,
            .complete = wrapper.complete,
        };
    }

    pub fn send(
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
        socket: os.socket_t,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            // valid for both send and shutdown callbacks
            // shudown returns res = 0 on success
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx: Context = @ptrFromInt(@intFromPtr(completion.context));
                var err: ?Error = if (ose == .SUCCESS) if (res == 0) Error.EOF else null else errno.toError(ose);
                if (err) |e| {
                    callback(ctx, e);
                } else {
                    callback(ctx, @as(usize, @intCast(res)));
                }
            }
        };
        return .{
            .args = .{ .send = .{ .socket = socket, .buffer = undefined } },
            .context = context,
            .complete = wrapper.complete,
        };
    }

    pub fn recv(
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
        socket: os.socket_t,
    ) Completion {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx: Context = @ptrFromInt(@intFromPtr(completion.context));
                var err: ?Error = if (ose == .SUCCESS) if (res == 0) Error.EOF else null else errno.toError(ose);
                if (err) |e| {
                    callback(ctx, e);
                } else {
                    callback(ctx, @as(usize, @intCast(res)));
                }
            }
        };
        return .{
            .args = .{ .recv = .{ .socket = socket, .buffer = undefined } },
            .context = context,
            .complete = wrapper.complete,
        };
    }
};

const testing = std.testing;

test "Completion accept success/failure callbacks" {
    // define Context
    const Context = struct {
        socket: os.socket_t = 0,
        err: ?anyerror = null,

        const Context = @This();

        fn acceptCallback(self: *Context, socket: Error!os.socket_t) void {
            self.socket = socket catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var ctx = Context{};
    var accept = Completion.accept(&ctx, Context.acceptCallback, 0);

    // test success callback
    _ = accept.complete(&accept, .SUCCESS, 123, 0);
    try testing.expectEqual(@as(os.socket_t, 123), ctx.socket);

    // test failure callback
    try testing.expect(ctx.err == null);
    _ = accept.complete(&accept, .PERM, 0, 0);
    try testing.expect(ctx.err != null);
    try testing.expect(ctx.err.? == Error.OperationNotPermitted);
}

test "size of Completion" {
    const print = std.debug.print;
    if (true) return error.SkipZigTest;

    print("\n", .{});
    print("Completion size: {d}\n", .{@sizeOf(Completion)});
    print("Args size: {d}\n", .{@sizeOf(Completion.Args)});
    print("Args bitSize: {d}\n", .{@bitSizeOf(Completion.Args)});
    print("net.Address size: {d}\n", .{@sizeOf(net.Address)});
    print("os.socket_t size: {d}\n", .{@sizeOf(os.socket_t)});
    print("os.socklen_t size: {d}\n", .{@sizeOf(os.socklen_t)});
    print("os.sockaddr size: {d}\n", .{@sizeOf(os.sockaddr)});

    print("[]const u8 size: {d}\n", .{@sizeOf([]const u8)});
    print("os.ShutdownHow size: {d}\n", .{@sizeOf(os.ShutdownHow)});

    print("os.linux.kernel_timespec size: {d}\n", .{@sizeOf(os.linux.kernel_timespec)});

    print("accept size: {d}\n", .{@sizeOf(struct {
        socket: os.socket_t,
        address_size: os.socklen_t = @sizeOf(os.sockaddr),
        address: os.sockaddr = undefined,
    })});

    print("read size: {d}\n", .{@sizeOf(struct {
        offset: u64,
        buffer: []u8,
        fd: os.fd_t,
    })});

    print("read aling: {d}\n", .{@alignOf(struct {
        offset: u64,
        buffer: []u8,
        fd: os.fd_t,
    })});

    print("align of buffer {d}\n", .{@alignOf([]u8)});
    print("align of u64 {d}\n", .{@alignOf(u64)});

    const ComplexTypeTag = enum {
        ok,
        not_ok,
    };
    print("ComplexTypeTag size {d}\n", .{@sizeOf(ComplexTypeTag)});
}
