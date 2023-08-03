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

        stream: tcp.Stream = undefined,

        buffer: [buffer_len * 2]u8 = undefined,
        head: usize = 0,
        tail: usize = 0,

        closed: bool = false,

        fn run(self: *Self) void {
            self.stream.bind(self, onRead, onWrite, onShutdown, onClose);
            // start reading
            self.stream.read(self.buffer[self.tail .. self.tail + recv_chunk]);
        }

        // when read completed
        // start new read until client signals no more data (shutdown of his write stream)
        fn onRead(self: *Self, no_bytes: usize) void {
            if (no_bytes != 0) {
                self.tail += no_bytes;
                self.stream.read(self.buffer[self.tail .. self.tail + recv_chunk]);
            }
            self.tryWrite();
        }

        fn tryWrite(self: *Self) void {
            if (!self.stream.writeReady()) return;
            if (self.head == self.tail) {
                if (self.stream.readClosed())
                    self.stream.shutdown();
                return;
            }
            self.stream.write(self.buffer[self.head..self.tail]);
        }

        fn onWrite(self: *Self, no_bytes: usize) void {
            if (no_bytes == 0) {
                unreachable;
            }
            self.head += no_bytes;
            self.tryWrite();
        }

        fn onShutdown(self: *Self, _: ?anyerror) void {
            self.stream.close();
        }

        fn onClose(self: *Self, _: ?anyerror) void {
            self.closed = true;
        }
    };

    // Accepts single connection and closes.
    const Server = struct {
        const Self = @This();

        listener: tcp.Listener = undefined,
        conn: Connection = undefined,
        closed: bool = false,

        fn run(self: *Self) void {
            self.listener.bind(self, onAccept, onClose);
            self.listener.accept(); // TODO ovdje moze ici how: single multishot
        }

        fn onAccept(self: *Self, stream: tcp.Stream) void {
            self.conn = .{ .stream = stream };
            self.conn.run();
            self.listener.close();
        }

        fn onClose(self: *Self) void {
            self.closed = true;
        }
    };

    // Connects to server.
    // Writes writer_buffer in send_chunks.
    // Reads everything send from the server into reader_buffer.
    // When finished sending closes write part, and waits for read part to be closed.
    const Client = struct {
        const Self = @This();

        stream: tcp.Stream = undefined,
        reader_buffer: [buffer_len * 3]u8 = undefined,
        reader_pos: usize = 0,

        writer_buffer: []const u8 = undefined,
        writer_pos: usize = 0,

        closed: bool = false,

        fn run(self: *Self) void {
            self.stream.bind(self, onRead, onWrite, onShutdown, onClose);
            self.stream.write(self.writer_buffer[self.writer_pos .. self.writer_pos + send_chunk]);
            self.stream.read(self.reader_buffer[self.reader_pos..]);
        }

        fn onRead(self: *Self, no_bytes: usize) void {
            if (no_bytes == 0) {
                self.stream.close();
                return;
            }
            self.reader_pos += no_bytes;
            self.stream.read(self.reader_buffer[self.reader_pos..]);
        }

        fn onWrite(self: *Self, no_bytes: usize) void {
            if (no_bytes == 0) {
                unreachable;
            }
            self.writer_pos += no_bytes;

            if (self.writer_pos >= self.writer_buffer.len) {
                self.stream.shutdown();
                return;
            }

            var to = self.writer_pos + send_chunk;
            if (to > self.writer_buffer.len) to = self.writer_buffer.len;
            self.stream.write(self.writer_buffer[self.writer_pos..to]);
        }

        fn onShutdown(self: *Self, _: ?anyerror) void {
            _ = self;
        }

        fn onClose(self: *Self, _: ?anyerror) void {
            self.closed = true;
        }
    };

    const buffer = [_]u8{ '0', '1', '2', '3', '4', '5', '6', '7' } ** (buffer_len / 8); // some random buffer

    // server
    var server_loop = try io.Loop.init(.{});
    defer server_loop.deinit();

    var address = try net.Address.parseIp4("127.0.0.1", 0);
    var server = Server{
        .listener = try tcp.Listener.init(&server_loop, &address),
    };
    server.run();

    // client
    var client_loop = try io.Loop.init(.{});
    defer client_loop.deinit();

    var client = Client{
        .stream = try tcp.Stream.initConnect(&client_loop, address),
        .writer_buffer = &buffer,
    };
    client.run();

    // start client in another thread
    const thr = try std.Thread.spawn(.{}, io.Loop.run, .{ &client_loop, io.Loop.RunMode.until_done });
    // server in this
    try server_loop.run(.until_done);
    thr.join();

    var conn = server.conn;
    try testing.expect(conn.stream.reader.closed());
    try testing.expect(conn.stream.reader.err.? == error.EOF);

    try testing.expect(client.stream.reader.err != null);
    try testing.expect(client.stream.reader.err.? == error.EOF);
    try testing.expect(client.stream.writer.err == null);

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
