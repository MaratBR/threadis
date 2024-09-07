const std = @import("std");
const aio = @import("aio");
const coro = @import("coro");

const Allocator = std.mem.Allocator;

const server = @import("socket/server.zig");
const Handler = @import("handler.zig");
const Store = @import("./store/store.zig").Store;
const Registry = @import("./client.zig").Registry;

pub const aio_options: aio.Options = .{
    .debug = false, // set to true to enable debug logs
};

pub const coro_options: coro.Options = .{
    .debug = false, // set to true to enable debug logs
};

pub const std_options: std.Options = .{
    .log_level = .debug,
};

pub fn main() !void {
    // TODO add arena allocator
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const sockfd = try server.create();
    defer server.close(sockfd);

    var store = try Store.init(allocator, .{ .segments_count = 16 });
    defer store.deinit();

    var client_registry = Registry.init(allocator);
    defer client_registry.deinit();

    var handler = try Handler.init(allocator, &store, &client_registry);
    defer handler.deinit();

    const server_thread = try server.startThread(allocator, sockfd, handler.connPipe());

    server_thread.join();
}
