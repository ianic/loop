const std = @import("std");
const os = std.os;
const net = std.net;

const io = @import("main.zig");
const tcp = io.tcp;

const assert = std.debug.assert;
const testing = std.testing;
const print = std.debug.print;

test "echo server" {
    const buffer_len = 4096;
    const send_chunk = 9;
    const recv_chunk = 7;

    const Connection = struct {
        const Self = @This();
        loop: *io.Loop,
        // operations
        stream: tcp.Stream = undefined,
        recv: tcp.Recv = undefined,
        send: tcp.Send = undefined,

        buffer: [buffer_len * 2]u8 = undefined,
        head: usize = 0,
        tail: usize = 0,

        fn start(self: *Self) void {
            self.recv = self.stream.reader(self, received);
            self.send = self.stream.writer(self, sent);
            self.recv.read(self.buffer[self.tail .. self.tail + recv_chunk]);
        }

        fn received(self: *Self, no_bytes_: io.Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.trySend();
                self.close();
                return;
            };
            self.tail += no_bytes;
            //print("server received {d} {d}\n", .{ no_bytes, self.tail });
            self.recv.read(self.buffer[self.tail .. self.tail + recv_chunk]);
            self.trySend();
        }

        fn trySend(self: *Self) void {
            if (!self.send.ready()) return;
            if (self.head == self.tail) {
                if (self.recv.closed())
                    self.send.shutdown();
                return;
            }
            self.send.write(self.buffer[self.head..self.tail]);
        }

        fn sent(self: *Self, no_bytes_: io.Error!usize) void {
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
                self.stream.close(self, closed);
            }
        }

        fn closed(self: *Self, _: io.Error!void) void {
            _ = self;
        }
    };

    const Server = struct {
        const Self = @This();
        loop: *io.Loop,
        //socket: os.socket_t = undefined, // accept socket
        listener: tcp.Listener = undefined,
        conn: Connection = undefined,

        fn listen(self: *Self, address: *net.Address) !void {
            self.listener = try tcp.Listener.init(self.loop, self, acceptCompleted, address);
            self.listener.accept(); // TODO ovdje moze ici how: single multishot
        }

        fn acceptCompleted(self: *Self, socket_: io.Error!os.socket_t) void {
            // TODO proslijedi ovdje io.Connection koji ima metode reader i writer u sebi ima socket i loop
            // i metodu close
            // i onda i u connection gore ide naming reader writer
            // read write shutdown => close
            var conn_socket = socket_ catch unreachable;
            self.conn = .{ .loop = self.loop, .stream = tcp.Stream.init(self.loop, conn_socket) };

            self.conn.start();
            self.close();
        }

        fn close(self: *Self) void {
            self.listener.close(self, closed);
        }

        fn closed(self: *Self, _: io.Error!void) void {
            _ = self;
        }
    };

    const Client = struct {
        const Self = @This();
        loop: *io.Loop,

        cli: tcp.Client = undefined,
        reader: tcp.Recv = undefined,
        writer: tcp.Send = undefined,

        reader_buffer: [buffer_len * 3]u8 = undefined,
        reader_pos: usize = 0,

        writer_buffer: []const u8 = undefined,
        writer_pos: usize = 0,

        fn connect(self: *Self, address: net.Address) !void {
            self.cli = tcp.Client.init(self.loop);
            try self.cli.connect(self, connectCompleted, address);
        }

        fn connectCompleted(self: *Self, result_: io.Error!os.socket_t) void {
            _ = result_ catch unreachable;

            self.reader = self.cli.reader(self, readCompleted);
            self.writer = self.cli.writer(self, writeCompleted);

            self.writer.write(self.writer_buffer[self.writer_pos .. self.writer_pos + send_chunk]);
            self.reader.read(self.reader_buffer[self.reader_pos..]);
        }

        fn readCompleted(self: *Self, no_bytes_: io.Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.close();
                //print("client received error {}\n", .{err});
                return;
            };
            self.reader_pos += no_bytes;
            //print("client received {d} {d}\n", .{ self.reader_pos, no_bytes });
            self.reader.read(self.reader_buffer[self.reader_pos..]);
        }

        fn writeCompleted(self: *Self, no_bytes_: io.Error!usize) void {
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
                self.cli.close(self, closeCompleted);
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
    try server.listen(&address);

    var client_loop = try io.Loop.init(.{});
    defer client_loop.deinit();
    var client = Client{ .loop = &client_loop, .writer_buffer = &buffer };
    try client.connect(address);
    const thr = try std.Thread.spawn(.{}, io.Loop.run, .{ &client_loop, io.Loop.RunMode.until_done });

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
