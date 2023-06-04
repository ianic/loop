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
const FIFO = @import("fifo.zig").FIFO;

const log = std.log;
const print = std.debug.print;

// prouci kada moze dobiti INTR: // This can happen while waiting for events with IORING_ENTER_GETEVENTS:
//  kaze u enter io_uring
//  rename ready() to submitReady()

test "size of Completion" {
    print("Completion size: {d}\n", .{@sizeOf(Completion)});
    print("Args size: {d}\n", .{@sizeOf(Completion.Args)});
    print("net.Address size: {d}\n", .{@sizeOf(net.Address)});
    print("os.socket_t size: {d}\n", .{@sizeOf(os.socket_t)});
}

const Completion = struct {
    /// This union encodes the set of operations supported as well as their arguments.
    const Args = union(enum) {
        none: void,
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
        shutdown: struct {
            socket: os.socket_t,
            how: os.ShutdownHow,
        },
    };

    const State = enum {
        initial,
        active,
        completed,
        closed,
    };

    next: ?*Completion = null, // used in fifo
    args: Args,
    state: State = .initial,
    context: ?*anyopaque,
    complete: *const fn (completion: *Completion, ose: os.E, res: i32, flags: u32) void,
    err: ?anyerror = null,

    fn ready(self: *Completion) bool {
        return self.state == .completed or self.state == .initial;
    }

    fn closed(self: *Completion) bool {
        return self.state == .closed;
    }

    fn setError(self: *Completion, err: anyerror) void {
        self.err = err;
        self.state = .closed;
    }

    fn prep(self: *Completion, sqe: *io_uring_sqe) void {
        switch (self.args) {
            .accept => |*args| {
                linux.io_uring_prep_accept(sqe, args.socket, &args.address, &args.address_size, os.SOCK.CLOEXEC);
            },
            .close => |args| {
                linux.io_uring_prep_close(sqe, args.fd);
            },
            .connect => |*args| {
                linux.io_uring_prep_connect(sqe, args.socket, &args.address.any, args.address.getOsSockLen());
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
                linux.io_uring_prep_shutdown(sqe, args.socket, @enumToInt(args.how));
            },
            .none => undefined,
        }
        sqe.user_data = @ptrToInt(self);
    }
};

const Error = error{
    EOF,
} || errno.Error;

const Accept = struct {
    loop: *Loop,
    completion: Completion,

    pub fn submit(self: *Accept) void {
        self.loop.submit(&self.completion);
    }

    pub fn ready(self: *Accept) bool {
        return self.completion.ready();
    }

    pub fn closed(self: *Accept) bool {
        return self.completion.closed();
    }

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), socket: Error!os.socket_t) void,
        socket: os.socket_t,
    ) Accept {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx = @intToPtr(Context, @ptrToInt(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    completion.setError(e);
                    callback(ctx, e);
                } else {
                    callback(ctx, @intCast(os.socket_t, res));
                }
            }
        };
        return .{
            .loop = loop,
            .completion = .{
                .args = .{ .accept = .{ .socket = socket } },
                .context = context,
                .complete = wrapper.complete,
            },
        };
    }
};

const Listener = struct {
    loop: *Loop,
    completion: Completion,
    address: net.Address,
    socket: os.socket_t,

    pub fn accept(self: *Listener) void {
        self.loop.submit(&self.completion);
    }

    pub fn ready(self: *Listener) bool {
        return self.completion.ready();
    }

    pub fn closed(self: *Listener) bool {
        return self.completion.closed();
    }

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), socket: Error!os.socket_t) void,
        address_: net.Address,
    ) !Listener {
        var address = address_;
        const socket = try listen(&address);

        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx = @intToPtr(Context, @ptrToInt(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    completion.setError(e);
                    callback(ctx, e);
                } else {
                    callback(ctx, @intCast(os.socket_t, res));
                }
            }
        };
        return .{
            .loop = loop,
            .address = address,
            .socket = socket,
            .completion = .{
                .args = .{ .accept = .{ .socket = socket } },
                .context = context,
                .complete = wrapper.complete,
            },
        };
    }
};

const Stream = struct {
    loop: *Loop,
    completion: Completion,
    socket: os.socket_t,

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

    pub fn close(
        self: *Stream,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!void) void,
    ) void {
        assert(self.completion.ready());

        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                _ = res;
                var ctx = @intToPtr(Context, @ptrToInt(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    completion.setError(e);
                    callback(ctx, e);
                } else {
                    callback(ctx, {});
                }
            }
        };

        self.completion = .{
            .args = .{ .close = .{ .fd = self.socket } },
            .context = context,
            .complete = wrapper.complete,
        };
        self.loop.submit(&self.completion);
    }

    pub fn ready(self: *Stream) bool {
        return self.completion.ready();
    }

    pub fn connect(self: *Stream) void {
        assert(self.completion.state == .initial);
        assert(self.completion.args == .connect);

        self.loop.submit(&self.completion);
    }

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), result: Error!void) void,
        address: net.Address,
    ) !Stream {
        const socket = try os.socket(address.any.family, os.SOCK.STREAM | os.SOCK.CLOEXEC, 0);
        errdefer os.closeSocket(socket);

        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                _ = res;
                var ctx = @intToPtr(Context, @ptrToInt(completion.context));
                var err: ?Error = if (ose == .SUCCESS) null else errno.toError(ose);
                if (err) |e| {
                    completion.setError(e);
                    callback(ctx, e);
                } else {
                    callback(ctx, {});
                }
            }
        };

        return .{
            .socket = socket,
            .loop = loop,
            .completion = .{
                .args = .{ .connect = .{ .socket = socket, .address = address } },
                .context = context,
                .complete = wrapper.complete,
            },
        };
    }
};

const Send = struct {
    loop: *Loop,
    completion: Completion,

    // sumbits completion to the loop
    pub fn submit(self: *Send, buffer: []const u8) void {
        assert(buffer.len > 0);
        self.completion.args.send.buffer = buffer;
        self.loop.submit(&self.completion);
    }

    pub fn write(self: *Send, buffer: []const u8) void {
        self.submit(buffer);
    }

    pub fn ready(self: *Send) bool {
        return self.completion.ready();
    }

    pub fn closed(self: *Send) bool {
        return self.completion.closed();
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

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
        socket: os.socket_t,
    ) Send {
        const Context = @TypeOf(context);
        const wrapper = struct {
            // valid for both send and shutdown callbacks
            // shudown returns res = 0 on success
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx = @intToPtr(Context, @ptrToInt(completion.context));
                var err: ?Error = if (ose == .SUCCESS) if (res == 0) Error.EOF else null else errno.toError(ose);
                if (err) |e| {
                    completion.setError(e);
                    callback(ctx, e);
                } else {
                    callback(ctx, @intCast(usize, res));
                }
            }
        };
        return .{
            .loop = loop,
            .completion = .{
                .args = .{ .send = .{ .socket = socket, .buffer = undefined } },
                .context = context,
                .complete = wrapper.complete,
            },
        };
    }
};

const Recv = struct {
    loop: *Loop,
    completion: Completion,

    pub fn submit(self: *Recv, buffer: []u8) void {
        assert(buffer.len > 0);
        self.completion.args.recv.buffer = buffer;
        self.loop.submit(&self.completion);
    }

    pub fn read(self: *Recv, buffer: []u8) void {
        self.submit(buffer);
    }

    pub fn ready(self: *Recv) bool {
        return self.completion.ready();
    }

    pub fn closed(self: *Recv) bool {
        return self.completion.closed();
    }

    // TODO: do we need recv shutdown
    // pub fn shutdown(self: *Recv) !void {
    //     self.completion.state = .closed; // TODO: make loop shutdown
    //     try os.shutdown(self.completion.args.recv.socket, .recv);
    // }

    pub fn init(
        loop: *Loop,
        context: anytype,
        comptime callback: fn (context: @TypeOf(context), no_bytes: Error!usize) void,
        socket: os.socket_t,
    ) Recv {
        const Context = @TypeOf(context);
        const wrapper = struct {
            fn complete(completion: *Completion, ose: os.E, res: i32, flags: u32) void {
                _ = flags;
                var ctx = @intToPtr(Context, @ptrToInt(completion.context));
                var err: ?Error = if (ose == .SUCCESS) if (res == 0) Error.EOF else null else errno.toError(ose);
                if (err) |e| {
                    completion.setError(e);
                    callback(ctx, e);
                } else {
                    callback(ctx, @intCast(usize, res));
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
};

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
    fn submit(self: *Loop, completion: *Completion) void {
        assert(completion.ready());
        completion.state = .active;
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
        var completed: u32 = 0;
        while (true) {
            // read completed from completion queue
            const len = self.ring.copy_cqes(&cqes, wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            self.in_kernel -= len;
            self.active -= len;
            completed += len;
            for (cqes[0..len]) |cqe| {
                // call completion callback
                const completion = @intToPtr(*Completion, @intCast(usize, cqe.user_data));
                completion.state = .completed;
                completion.complete(completion, cqe.err(), cqe.res, cqe.flags);
            }
            if (len < cqes.len) return completed;
        }
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
    var loop: Loop = undefined;
    var accept = Accept.init(&loop, &ctx, Context.acceptCallback, 0);

    // test success callback
    _ = accept.completion.complete(&accept.completion, .SUCCESS, 123, 0);
    try testing.expectEqual(@as(os.socket_t, 123), ctx.socket);

    // test failure callback
    try testing.expect(ctx.err == null);
    _ = accept.completion.complete(&accept.completion, .PERM, 0, 0);
    try testing.expect(ctx.err != null);
    try testing.expect(ctx.err.? == Error.OperationNotPermitted);
}

test "accept" {
    var address = try net.Address.parseIp4("127.0.0.1", 0);
    const listener_socket = try listen(&address);
    var loop = try Loop.init(.{});
    defer loop.deinit();

    const Context = struct {
        const Self = @This();
        calls: usize = 0,

        fn acceptCallback(self: *Self, socket: Error!os.socket_t) void {
            _ = socket catch unreachable;
            self.calls += 1;
        }
    };
    var ctx = Context{};
    var accept = Accept.init(&loop, &ctx, Context.acceptCallback, listener_socket);
    try testing.expectEqual(@as(usize, 0), loop.active);
    accept.submit();
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

const io = @This();

test "echo server" {
    const buffer_len = 4096;
    const send_chunk = 9;
    const recv_chunk = 7;

    const Connection = struct {
        const Self = @This();
        loop: *io.Loop,
        socket: os.socket_t = undefined, // client connection socket
        // operations
        recv: io.Recv = undefined,
        send: io.Send = undefined,

        buffer: [buffer_len * 2]u8 = undefined,
        head: usize = 0,
        tail: usize = 0,

        fn start(self: *Self) void {
            self.recv = io.Recv.init(self.loop, self, received, self.socket);
            self.send = io.Send.init(self.loop, self, sent, self.socket);
            self.recv.submit(self.buffer[self.tail .. self.tail + recv_chunk]);
        }

        fn received(self: *Self, no_bytes_: Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.trySend();
                self.close();
                return;
            };
            self.tail += no_bytes;
            //print("server received {d} {d}\n", .{ no_bytes, self.tail });
            self.recv.submit(self.buffer[self.tail .. self.tail + recv_chunk]);
            self.trySend();
        }

        fn trySend(self: *Self) void {
            if (!self.send.ready()) return;
            if (self.head == self.tail) {
                if (self.recv.closed())
                    self.send.shutdown();
                return;
            }
            self.send.submit(self.buffer[self.head..self.tail]);
        }

        fn sent(self: *Self, no_bytes_: Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.close();
                return;
            };
            self.head += no_bytes;
            //print("server sent {d}\n", .{self.head});
            self.trySend();
        }

        fn close(self: *Self) void {
            if (self.send.closed() and self.recv.closed()) {
                os.closeSocket(self.socket);
            }
        }
    };

    const Server = struct {
        const Self = @This();
        loop: *io.Loop,
        //socket: os.socket_t = undefined, // accept socket
        listener: io.Listener = undefined,
        conn: Connection = undefined,

        fn listen(self: *Self, address: net.Address) !void {
            self.listener = try io.Listener.init(self.loop, self, acceptCompleted, address);
            self.listener.accept(); // TODO ovdje moze ici how: single multishot
        }

        fn acceptCompleted(self: *Self, socket_: Error!os.socket_t) void {
            // TODO proslijedi ovdje io.Connection koji ima metode reader i writer u sebi ima socket i loop
            // i metodu close
            // i onda i u connection gore ide naming reader writer
            // read write shutdown => close
            var conn_socket = socket_ catch unreachable;
            self.conn = .{ .loop = self.loop, .socket = conn_socket };
            self.conn.start();
        }

        fn close(self: *Self) void {
            // TODO
            os.closeSocket(self.listener.socket);
        }
    };

    const Client = struct {
        const Self = @This();
        loop: *io.Loop,

        // operations
        stream: io.Stream = undefined,
        reader: io.Recv = undefined,
        writer: io.Send = undefined,

        reader_buffer: [buffer_len * 3]u8 = undefined,
        reader_pos: usize = 0,

        writer_buffer: []const u8 = undefined,
        writer_pos: usize = 0,

        fn connect(self: *Self, address: net.Address) !void {
            self.stream = try io.Stream.init(self.loop, self, connectCompleted, address);
            self.stream.connect();
        }

        fn connectCompleted(self: *Self, result_: io.Error!void) void {
            _ = result_ catch unreachable;

            self.reader = self.stream.reader(self, readCompleted);
            self.writer = self.stream.writer(self, writeCompleted);

            self.writer.write(self.writer_buffer[self.writer_pos .. self.writer_pos + send_chunk]);
            self.reader.read(self.reader_buffer[self.reader_pos..]);
        }

        fn readCompleted(self: *Self, no_bytes_: Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.close();
                //print("client received error {}\n", .{err});
                return;
            };
            self.reader_pos += no_bytes;
            //print("client received {d} {d}\n", .{ self.reader_pos, no_bytes });
            self.reader.read(self.reader_buffer[self.reader_pos..]);
        }

        fn writeCompleted(self: *Self, no_bytes_: Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.close();
                return;
            };
            self.writer_pos += no_bytes;
            //print("client sent {d} {d}\n", .{ self.writer_pos, no_bytes });

            if (self.writer_pos >= self.writer_buffer.len) {
                self.writer.close();
                return;
            }

            var to = self.writer_pos + send_chunk;
            if (to > self.writer_buffer.len) to = self.writer_buffer.len;
            self.writer.write(self.writer_buffer[self.writer_pos..to]);
        }

        fn close(self: *Self) void {
            if (self.writer.closed() and self.reader.closed()) {
                //os.closeSocket(self.socket);
                self.stream.close(self, closeCompleted);
            }
        }

        fn closeCompleted(self: *Self, _: io.Error!void) void {
            _ = self;
        }
    };

    const buffer = [_]u8{ '0', '1', '2', '3', '4', '5', '6', '7' } ** (buffer_len / 8);

    var loop = try io.Loop.init(.{});
    defer loop.deinit();

    var address = try net.Address.parseIp4("127.0.0.1", 0);
    var server = Server{ .loop = &loop };
    try server.listen(address);
    defer server.close();
    address = server.listener.address; // because we have OS chosen port

    var client_loop = try io.Loop.init(.{});
    defer client_loop.deinit();
    var client = Client{ .loop = &client_loop, .writer_buffer = &buffer };
    try client.connect(address);
    const thr = try std.Thread.spawn(.{}, io.Loop.run, .{ &client_loop, Loop.RunMode.until_done });

    try loop.run(.until_done);
    thr.join();

    try testing.expect(server.conn.recv.closed());
    try testing.expect(server.conn.send.closed());

    try testing.expect(client.reader.closed());
    try testing.expect(client.writer.closed());

    try testing.expectEqual(buffer.len, server.conn.tail);
    try testing.expectEqual(buffer.len, server.conn.head);
    try testing.expectEqual(buffer.len, client.reader_pos);
    try testing.expectEqualSlices(u8, &buffer, client.reader_buffer[0..client.reader_pos]);
}
