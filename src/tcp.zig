const std = @import("std");
const os = std.os;
const net = std.net;
const mem = std.mem;

const assert = std.debug.assert;
const testing = std.testing;

const Completion = @import("completion.zig").Completion;
const Error = @import("completion.zig").Error;
const Loop = @import("loop.zig").Loop;

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
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
    ) Recv {
        return Recv.init(self.loop, context, callback, self.socket());
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
    completion: Completion,
    socket: os.socket_t,

    pub fn init(loop: *Loop, socket: os.socket_t) Stream {
        return .{ .loop = loop, .socket = socket, .completion = undefined };
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

    pub fn reader(
        self: *Stream,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
    ) Recv {
        return Recv.init(self.loop, context, callback, self.socket);
    }

    pub fn writer(
        self: *Stream,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
    ) Send {
        return Send.init(self.loop, context, callback, self.socket);
    }
};

pub const Send = struct {
    loop: *Loop,
    completion: Completion,

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
    completion: Completion,

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
        socket: os.socket_t,
    ) Recv {
        return .{
            .loop = loop,
            .completion = Completion.recv(context, callback, socket),
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
