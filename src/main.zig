const std = @import("std");
const aio = @import("aio");
const coro = @import("coro");
const log = std.log.scoped(.coro_aio);

const srv = @import("server.zig");
const Server = srv.Server;
const Queue = @import("queue.zig").Queue(Server.Connection);

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
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .{};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var conn_queue = try Queue.init(allocator);
    defer conn_queue.deinit();

    const server_addr = std.net.Address.parseIp("0.0.0.0", 4679) catch |err| {
        std.debug.panic("failed to parse server ip: {}", .{err});
    };

    var thread_pool = try @import("thread_pool.zig").init(allocator, 4);
    defer thread_pool.deinit();

    try thread_pool.start(connectionThreadLoop, .{&conn_queue});

    startServer(&conn_queue, server_addr) catch |err| {
        std.debug.panic("fatal error while listening on the socket/starting the server: {}", .{err});
    };
}

fn startServer(queue: *Queue, addr: std.net.Address) !void {
    var server_instance = srv.Server.init(.{ .addr = addr });
    try server_instance.start();

    while (true) {
        const conn = try server_instance.accept();

        queue.push(conn) catch |err| {
            std.debug.panic("failed to queue connection: {}", .{err});
        };
    }
}

fn connectionThreadLoop(queue: *Queue) void {
    var maybe_connection: ?Server.Connection = null;

    // TODO add a way to interrupt the loop
    while (true) {
        while (maybe_connection == null) {
            maybe_connection = queue.popWait(60_000);
        }

        const connection = maybe_connection.?;

        handleConnection(connection);
    }
}

fn handleConnection(c: Server.Connection) void {
    var connection = c;
    connection.close() catch |err| {
        std.log.err("failed to close the connection: {}", .{err});
    };
}
