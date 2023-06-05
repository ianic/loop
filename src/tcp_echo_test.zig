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

    // Server side echo connection.
    // Reads into buffer and moves head pointer on every read.
    // Write moves tail buffer pointer.
    // Loops until reader is closed than finishes all writes and closes write part.
    const Connection = struct {
        const Self = @This();
        loop: *io.Loop,

        stream: tcp.Stream = undefined,
        reader: tcp.Recv = undefined,
        writer: tcp.Send = undefined,

        buffer: [buffer_len * 2]u8 = undefined,
        head: usize = 0,
        tail: usize = 0,

        closed: bool = false,

        fn start(self: *Self) void {
            self.reader = self.stream.reader(self, readCompleted);
            self.writer = self.stream.writer(self, writeCompleted);
            self.reader.read(self.buffer[self.tail .. self.tail + recv_chunk]);
        }

        fn readCompleted(self: *Self, no_bytes_: io.Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.tryWrite();
                self.close();
                return;
            };
            self.tail += no_bytes;
            self.reader.read(self.buffer[self.tail .. self.tail + recv_chunk]);
            self.tryWrite();
        }

        fn tryWrite(self: *Self) void {
            if (!self.writer.ready()) return;
            if (self.head == self.tail) {
                if (self.reader.closed())
                    self.writer.close();
                return;
            }
            self.writer.write(self.buffer[self.head..self.tail]);
        }

        fn writeCompleted(self: *Self, no_bytes_: io.Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.close();
                return;
            };
            self.head += no_bytes;
            self.tryWrite();
        }

        fn close(self: *Self) void {
            if (self.writer.closed() and self.reader.closed()) {
                self.stream.close(self, closeCompleted);
            }
        }

        fn closeCompleted(self: *Self, _: io.Error!void) void {
            self.closed = true;
        }
    };

    // Accepts single connection and closes.
    const Server = struct {
        const Self = @This();
        loop: *io.Loop,

        listener: tcp.Listener = undefined,
        conn: Connection = undefined,

        closed: bool = false,

        fn listen(self: *Self, address: *net.Address) !void {
            self.listener = try tcp.Listener.init(self.loop, self, acceptCompleted, address);
            self.listener.accept(); // TODO ovdje moze ici how: single multishot
        }

        fn acceptCompleted(self: *Self, socket_: io.Error!os.socket_t) void {
            var conn_socket = socket_ catch unreachable;
            self.conn = .{ .loop = self.loop, .stream = tcp.Stream.init(self.loop, conn_socket) };

            self.conn.start();
            self.close();
        }

        fn close(self: *Self) void {
            self.listener.close(self, closeCompleted);
        }

        fn closeCompleted(self: *Self, _: io.Error!void) void {
            self.closed = true;
        }
    };

    // Connects to server.
    // Writes writer_buffer in send_chunks.
    // Reads everything send from the server into reader_buffer.
    // When finished sending closes write part, and waits for read part to be closed.
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

        closed: bool = false,

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
                return;
            };
            self.reader_pos += no_bytes;
            self.reader.read(self.reader_buffer[self.reader_pos..]);
        }

        fn writeCompleted(self: *Self, no_bytes_: io.Error!usize) void {
            const no_bytes = no_bytes_ catch {
                self.close();
                return;
            };
            self.writer_pos += no_bytes;

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
            self.closed = true;
        }
    };

    const buffer = [_]u8{ '0', '1', '2', '3', '4', '5', '6', '7' } ** (buffer_len / 8); // some random buffer

    // server
    var server_loop = try io.Loop.init(.{});
    defer server_loop.deinit();
    var address = try net.Address.parseIp4("127.0.0.1", 0);
    var server = Server{ .loop = &server_loop };
    try server.listen(&address);

    // client
    var client_loop = try io.Loop.init(.{});
    defer client_loop.deinit();
    var client = Client{ .loop = &client_loop, .writer_buffer = &buffer };
    try client.connect(address);

    // start client in another thread
    const thr = try std.Thread.spawn(.{}, io.Loop.run, .{ &client_loop, io.Loop.RunMode.until_done });
    // server in this
    try server_loop.run(.until_done);
    thr.join();

    var conn = server.conn;
    try testing.expect(conn.reader.closed());
    try testing.expect(conn.writer.closed());

    try testing.expect(client.reader.closed());
    try testing.expect(client.writer.closed());

    // expect buffer echoed back to the client
    try testing.expectEqual(buffer.len, conn.tail);
    try testing.expectEqual(buffer.len, conn.head);
    try testing.expectEqual(buffer.len, client.reader_pos);
    try testing.expectEqualSlices(u8, &buffer, conn.buffer[0..conn.tail]); // conn received all data
    try testing.expectEqualSlices(u8, &buffer, client.reader_buffer[0..client.reader_pos]); // and return that to client

    // expect all closedCompleted callbacks to be called
    try testing.expect(client.closed);
    try testing.expect(conn.closed);
    try testing.expect(server.closed);
}
