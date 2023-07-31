const std = @import("std");
const os = std.os;
const net = std.net;
const mem = std.mem;

const assert = std.debug.assert;
const testing = std.testing;

const Completion = @import("completion.zig").Completion;
const Error = @import("completion.zig").Error;
const Loop = @import("loop.zig").Loop;
const errno = @import("errno.zig");

pub const Listener = struct {
    loop: *Loop,
    completion: Completion,

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), socket: Error!os.socket_t) void,
        address: *net.Address,
    ) !Listener {
        const socket = try listen(address);
        return .{
            .loop = loop,
            .completion = Completion.accept(context, callback, socket),
        };
    }

    pub fn accept(self: *Listener) void {
        assert(self.completion.args == .accept);
        self.loop.submit(&self.completion);
    }

    pub fn stream(self: *Listener, socket: os.socket_t) Stream {
        return Stream.init(self.loop, socket);
    }

    pub fn close(
        self: *Listener,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!void) void,
    ) void {
        assert(self.completion.args == .accept);
        assert(self.completion.ready());
        const socket = self.completion.args.accept.socket;
        self.completion = Completion.close(context, callback, socket);
        self.loop.submit(&self.completion);
    }
};

pub const Client = struct {
    loop: *Loop,
    completion: Completion,

    pub fn init(loop: *Loop) Client {
        return .{
            .loop = loop,
            .completion = .{
                .args = .{ .connect = undefined },
                .context = undefined,
                .complete = undefined,
            },
        };
    }

    pub fn connect(
        self: *Client,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), socket: Error!os.socket_t) void,
        address: net.Address,
    ) !void {
        assert(self.completion.args == .connect or
            self.completion.args == .close);
        assert(self.completion.ready());

        const sock = try os.socket(address.any.family, os.SOCK.STREAM | os.SOCK.CLOEXEC, 0);
        errdefer os.closeSocket(sock);
        self.completion = Completion.connect(context, callback, sock, address);
        self.loop.submit(&self.completion);
    }

    fn socket(self: *Client) os.socket_t {
        assert(self.completion.args == .connect);
        return self.completion.args.connect.socket;
    }

    pub fn reader(
        self: *Client,
        context: anytype,
        comptime resolve: fn (context: @TypeOf(context), usize) void,
        comptime reject: fn (context: @TypeOf(context), anyerror) void,
    ) Recv {
        return Recv.init(self.loop, context, resolve, reject, self.socket());
    }

    pub fn writer(
        self: *Client,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
    ) Send {
        return Send.init(self.loop, context, callback, self.socket());
    }

    pub fn close(
        self: *Client,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!void) void,
    ) void {
        assert(self.completion.ready());
        self.completion = Completion.close(context, callback, self.socket());
        self.loop.submit(&self.completion);
    }
};

pub const Stream = struct {
    loop: *Loop,
    completion: Completion = undefined,
    socket: os.socket_t,

    reader: Recv = undefined,
    writer: Send = undefined,

    pub fn init(loop: *Loop, socket: os.socket_t) Stream {
        return .{ .loop = loop, .socket = socket };
    }

    pub fn bind(
        self: *Stream,
        context: anytype,
        comptime readResolve: fn (context: @TypeOf(context), usize) void,
        comptime writeResolve: fn (context: @TypeOf(context), usize) void,
    ) void {
        self.reader = Recv{ .loop = self.loop };
        self.reader.bind(self.socket, context, readResolve);
        self.writer = Send{ .loop = self.loop };
        self.writer.bind(self.socket, context, writeResolve);
    }

    pub fn close(
        self: *Stream,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!void) void,
    ) void {
        assert(self.completion.ready());
        self.completion = Completion.close(context, callback, self.socket);
        self.loop.submit(&self.completion);
    }

    // pub fn writer(
    //     self: *Stream,
    //     context: anytype,
    //     comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
    // ) Send {
    //     return Send.init(self.loop, context, callback, self.socket);
    // }
};

pub const Send = struct {
    loop: *Loop,
    completion: Completion = undefined,

    context: *anyopaque = undefined,
    err: ?anyerror = null,

    pub fn bind(
        self: *Send,
        socket: os.socket_t,
        context: anytype,
        comptime resolve: fn (context: @TypeOf(context), usize) void,
    ) void {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var send: *Send = @alignCast(@ptrCast(completion.context));
                var ctx: Context = @alignCast(@ptrCast(send.context));
                if (ose == .SUCCESS) {
                    const no_bytes: usize = @intCast(res);
                    if (no_bytes == 0) {
                        send.err = error.EOF;
                    }
                    resolve(ctx, no_bytes);
                } else {
                    send.err = errno.toError(ose);
                    resolve(ctx, 0);
                }
            }
        };
        self.context = context;
        self.completion = .{
            .args = .{ .send = .{ .socket = socket, .buffer = undefined } },
            .context = self,
            .complete = wrapper.complete,
        };
    }

    pub fn closed(self: *Send) bool {
        return self.err != null;
    }

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
        socket: os.socket_t,
    ) Send {
        return .{
            .loop = loop,
            .completion = Completion.send(context, callback, socket),
        };
    }

    pub fn write(self: *Send, buffer: []const u8) void {
        assert(buffer.len > 0);
        self.completion.args.send.buffer = buffer;
        self.loop.submit(&self.completion);
    }

    pub fn ready(self: *Send) bool {
        return self.completion.ready();
    }

    // changes completion from send to shutdown and submits completion
    // can't use completion after this
    pub fn shutdown(self: *Send) void {
        assert(self.completion.args == .send);
        const socket = self.completion.args.send.socket;
        self.completion.args = .{ .shutdown = .{ .socket = socket, .how = .send } };
        self.loop.submit(&self.completion);
    }

    pub fn close(self: *Send) void {
        self.shutdown();
    }
};

pub const Recv = struct {
    loop: *Loop,
    completion: Completion = undefined,

    context: *anyopaque = undefined,
    err: ?anyerror = null,

    pub fn bind(
        self: *Recv,
        socket: os.socket_t,
        context: anytype,
        comptime resolve: fn (context: @TypeOf(context), usize) void,
    ) void {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var recv: *Recv = @alignCast(@ptrCast(completion.context));
                var ctx: Context = @alignCast(@ptrCast(recv.context));
                if (ose == .SUCCESS) {
                    const no_bytes: usize = @intCast(res);
                    if (no_bytes == 0) {
                        recv.err = error.EOF;
                    }
                    resolve(ctx, no_bytes);
                } else {
                    recv.err = errno.toError(ose);
                    resolve(ctx, 0);
                }
            }
        };
        self.context = context;
        self.completion = .{
            .args = .{ .recv = .{ .socket = socket, .buffer = undefined } },
            .context = self,
            .complete = wrapper.complete,
        };
    }

    pub fn closed(self: *Recv) bool {
        return self.err != null;
    }

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime resolve: fn (context: @TypeOf(context), usize) void,
        comptime reject: fn (context: @TypeOf(context), anyerror) void,
        socket: os.socket_t,
    ) Recv {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx: Context = @alignCast(@ptrCast(completion.context));
                if (ose == .SUCCESS) {
                    resolve(ctx, @intCast(res));
                } else {
                    reject(ctx, errno.toError(ose));
                }
            }
        };
        return .{
            .loop = loop,
            .completion = .{
                .args = .{ .recv = .{ .socket = socket, .buffer = undefined } },
                .context = context,
                .complete = wrapper.complete,
            },
        };
    }

    pub fn read(self: *Recv, buffer: []u8) void {
        assert(buffer.len > 0);
        self.completion.args.recv.buffer = buffer;
        self.loop.submit(&self.completion);
    }

    pub fn ready(self: *Recv) bool {
        return self.completion.ready();
    }

    //pub fn err(self: *Recf) ?anyerror {}
};

pub fn listen(address: *net.Address) !os.socket_t {
    const kernel_backlog = 1;
    const socket = try os.socket(address.any.family, os.SOCK.STREAM | os.SOCK.CLOEXEC, 0);
    errdefer os.closeSocket(socket);

    try os.setsockopt(socket, os.SOL.SOCKET, os.SO.REUSEADDR, &mem.toBytes(@as(c_int, 1)));
    try os.bind(socket, &address.any, address.getOsSockLen());
    try os.listen(socket, kernel_backlog);

    if (address.getPort() == 0) {
        // If we were called with port 0
        // set address to the OS-chosen IP/port from socket.
        var slen: os.socklen_t = address.getOsSockLen();
        try os.getsockname(socket, &address.any, &slen);
    }
    return socket;
}
