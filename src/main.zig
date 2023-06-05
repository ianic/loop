const std = @import("std");
const os = std.os;
const net = std.net;

const assert = std.debug.assert;
const testing = std.testing;
const print = std.debug.print;

pub const Error = @import("completion.zig").Error;
pub const Loop = @import("loop.zig").Loop;
pub const tcp = @import("tcp.zig");

// prouci kada moze dobiti INTR: // This can happen while waiting for events with IORING_ENTER_GETEVENTS:
//  kaze u enter io_uring

const io = @This();

test "listener accepts connection" {
    var address = try net.Address.parseIp4("127.0.0.1", 0);
    var loop = try io.Loop.init(.{});
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
    var listener = try tcp.Listener.init(&loop, &ctx, Context.acceptCallback, &address);
    try testing.expectEqual(@as(usize, 0), loop.active);
    listener.accept();
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

test "import test files" {
    _ = @import("tcp_echo_test.zig");
}
