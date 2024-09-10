const std = @import("std");
const aio = @import("aio");
const coro = @import("coro");
const builtin = @import("builtin");

const Allocator = std.mem.Allocator;

const server = @import("socket/server.zig");
const Handler = @import("handler.zig");
const Store = @import("./store/store.zig").Store;
const Registry = @import("./client.zig").Registry;

pub const aio_options: aio.Options = .{
    .debug = false, // set to true to enable debug logs, WILL SEGFAULT IF SET TO TRUE
};

pub const coro_options: coro.Options = .{
    .debug = false, // set to true to enable debug logs
};

pub const std_options: std.Options = .{
    .log_level = .debug,
};

const log = std.log.scoped(.main);

pub fn main() !void {
    if (builtin.target.os.tag == .windows) {
        const utf8_codepage: c_uint = 65001;
        _ = std.os.windows.kernel32.SetConsoleOutputCP(utf8_codepage);
    }

    // TODO add arena allocator
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var store = try Store.init(allocator, .{ .segments_count = 16 });
    defer store.deinit();

    var client_registry = Registry.init(allocator);
    defer client_registry.deinit();

    var sched = try coro.Scheduler.init(allocator, .{});
    defer sched.deinit();
    var thread_pool = try coro.ThreadPool.init(gpa.allocator(), .{});
    defer thread_pool.deinit();

    var handler = try Handler.init(allocator, &store, &client_registry, &sched, &thread_pool);
    defer handler.deinit();

    const server_options = getServerOptions();
    _ = try sched.spawn(server.server, .{ server_options, handler.connPipe() }, .{});

    var sched_thread = try std.Thread.spawn(.{ .allocator = allocator }, runSched, .{&sched});
    sched_thread.join();
}

fn runSched(sched: *coro.Scheduler) void {
    log.debug("running scheduler on thread {}", .{std.Thread.getCurrentId()});
    sched.run(.wait) catch |err| {
        log.err("error while running scheduler: {}", .{err});
    };
    log.debug("scheduler thread exited", .{});
}

fn getServerOptions() server.Options {
    const server_addr = std.net.Address.parseIp("127.0.0.1", 6000) catch |err| {
        std.debug.panic("failed to parse server ip: {}", .{err});
    };
    return .{ .addr = server_addr };
}
