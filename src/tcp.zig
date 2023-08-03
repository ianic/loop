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
    socket: os.socket_t,

    completion: Completion = undefined,
    context: *anyopaque = undefined,
    err: ?anyerror = null,

    pub fn init(loop: *Loop, address: *net.Address) !Listener {
        const socket = try listen(address);
        return Listener{ .loop = loop, .socket = socket };
    }

    pub fn bind(
        self: *Listener,
        context: anytype,
        comptime onAccept: fn (context: @TypeOf(context), Stream) void,
        comptime onClose: fn (context: @TypeOf(context)) void,
    ) void {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var listener: *Listener = @alignCast(@ptrCast(completion.context));
                var ctx: Context = @alignCast(@ptrCast(listener.context));
                if (completion.args == .accept) {
                    switch (ose) {
                        .SUCCESS => {
                            const socket = @as(os.socket_t, @intCast(res));
                            const stream = Stream.init(listener.loop, socket);
                            onAccept(ctx, stream);
                        },
                        // retry cases
                        // reference: https://man7.org/linux/man-pages/man2/accept4.2.html
                        .NETDOWN, .PROTO, .NOPROTOOPT, .HOSTDOWN, .NONET, .HOSTUNREACH, .OPNOTSUPP, .NETUNREACH => {
                            // TODO: log this case
                            listener.accept();
                        },
                        else => {
                            listener.err = errno.toError(ose);
                            listener.close();
                        },
                    }
                    return;
                }
                if (completion.args == .close) {
                    onClose(ctx);
                    return;
                }
                unreachable;
            }
        };
        self.context = context;
        self.completion = .{
            .args = .{ .accept = .{ .socket = self.socket } },
            .context = self,
            .complete = wrapper.complete,
        };
    }

    pub fn accept(self: *Listener) void {
        assert(self.completion.args == .accept);
        self.loop.submit(&self.completion);
    }

    // changes completion from accept to close
    pub fn close(self: *Listener) void {
        assert(self.completion.args == .accept);
        self.completion.args = .{ .close = .{ .fd = self.socket } };
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
    socket: os.socket_t,

    reader: Recv = undefined,
    writer: Send = undefined,

    pub fn init(loop: *Loop, socket: os.socket_t) Stream {
        return .{ .loop = loop, .socket = socket };
    }

    pub fn initConnect(loop: *Loop, address: net.Address) !Stream {
        return Stream.init(loop, try connect(address));
    }

    pub fn bind(
        self: *Stream,
        context: anytype,
        comptime onRead: fn (context: @TypeOf(context), usize) void,
        comptime onWrite: fn (context: @TypeOf(context), usize) void,
        comptime onShutdown: fn (context: @TypeOf(context), ?anyerror) void,
        comptime onClose: fn (context: @TypeOf(context), ?anyerror) void,
    ) void {
        self.reader = Recv{ .loop = self.loop };
        self.reader.bind(self.socket, context, onRead);
        self.writer = Send{ .loop = self.loop };
        self.writer.bind(self.socket, context, onWrite, onShutdown, onClose);
    }

    pub fn write(self: *Stream, buffer: []const u8) void {
        self.writer.write(buffer);
    }

    pub fn read(self: *Stream, buffer: []u8) void {
        self.reader.read(buffer);
    }

    pub fn writeReady(self: *Stream) bool {
        return self.writer.ready();
    }

    pub fn readReady(self: *Stream) bool {
        return self.reader.ready();
    }

    pub fn readClosed(self: *Stream) bool {
        return self.reader.closed();
    }

    pub fn shutdown(self: *Stream) void {
        self.writer.shutdown();
    }

    pub fn close(self: *Stream) void {
        self.writer.close();
    }
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
        comptime onWrite: fn (context: @TypeOf(context), usize) void,
        comptime onShutdown: fn (context: @TypeOf(context), ?anyerror) void,
        comptime onClose: fn (context: @TypeOf(context), ?anyerror) void,
    ) void {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var send: *Send = @alignCast(@ptrCast(completion.context));
                var ctx: Context = @alignCast(@ptrCast(send.context));
                const err: ?anyerror = if (ose == .SUCCESS) null else errno.toError(ose);
                if (completion.args == .shutdown) {
                    onShutdown(ctx, err);
                    return;
                }
                if (completion.args == .close) {
                    onClose(ctx, err);
                    return;
                }
                if (completion.args == .send) {
                    if (ose == .SUCCESS) {
                        const no_bytes: usize = @intCast(res);
                        if (no_bytes == 0) {
                            // TODO treba li ovo
                            send.err = error.EOF;
                        }
                        onWrite(ctx, no_bytes);
                    } else {
                        send.err = errno.toError(ose);
                        onWrite(ctx, 0);
                    }
                    return;
                }
                unreachable;
            }
        };
        self.context = context;
        self.completion = .{
            .args = .{ .send = .{ .socket = socket, .buffer = undefined } },
            .context = self,
            .complete = wrapper.complete,
        };
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
        return self.completion.args == .send and self.completion.ready();
    }

    // changes completion from send to shutdown and submits completion
    // can't use completion after this
    pub fn shutdown(self: *Send) void {
        assert(self.completion.args == .send);
        const socket = self.completion.args.send.socket;
        self.completion.args = .{ .shutdown = .{ .socket = socket, .how = .send } };
        self.loop.submit(&self.completion);
    }

    // changes completion from shutdown to close
    pub fn close(self: *Send) void {
        assert(self.completion.args == .shutdown);
        const socket = self.completion.args.shutdown.socket;
        self.completion.args = .{ .close = .{ .fd = socket } };
        self.loop.submit(&self.completion);
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
                    if (no_bytes == 0) recv.err = error.EOF;
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

    pub fn read(self: *Recv, buffer: []u8) void {
        assert(buffer.len > 0);
        self.completion.args.recv.buffer = buffer;
        self.loop.submit(&self.completion);
    }

    pub fn ready(self: *Recv) bool {
        return self.completion.ready() and self.err == null;
    }
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

pub fn connect(address: net.Address) !os.socket_t {
    const stream = try std.net.tcpConnectToAddress(address);
    return stream.handle;
}
