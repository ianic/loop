const std = @import("std");
const os = std.os;
const net = std.net;

const assert = std.debug.assert;
const testing = std.testing;

const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

const errno = @import("errno.zig");

test "Completion complete" {
    const Ctx = struct {
        calls: usize = 0,

        fn callback(self: *@This(), result: Error!os.socket_t) void {
            _ = result catch unreachable;
            self.calls += 1;
        }
    };
    var ctx = Ctx{};

    var completion = Completion{
        .operation = .{ .accept = .{ .socket = 0, .address = undefined } },
        .result = 0,
        .context = &ctx,
        .callback = (struct {
            fn callback(completion: *Completion) void {
                const result = if (completion.result < 0)
                    errno.toError(@intToEnum(os.E, -completion.result))
                else
                    @intCast(os.socket_t, completion.result);

                @call(.auto, Ctx.callback, .{
                    @intToPtr(*Ctx, @ptrToInt(completion.context)),
                    @intToPtr(*const Error!os.socket_t, @ptrToInt(&result)).*,
                });
            }
        }).callback,
    };

    completion.complete();
    try testing.expectEqual(@as(usize, 1), ctx.calls);
}

pub const Error = error{
    Canceled,
} || errno.Error;

const Completion = struct {
    operation: Operation,
    result: i32 = undefined,
    context: ?*anyopaque,
    callback: *const fn (completion: *Completion) void,

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
    ring: IO_Uring,
};
