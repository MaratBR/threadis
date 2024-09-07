const std = @import("std");
const posix = std.posix;
const util = @import("./util.zig");

fn getSocketOptions() util.Options {
    const server_addr = std.net.Address.parseIp("127.0.0.1", 4689) catch |err| {
        std.debug.panic("failed to parse server ip: {}", .{err});
    };
    return .{ .addr = server_addr };
}

pub fn create() !posix.socket_t {
    const options = getSocketOptions();
    const sockfd = try util.create(options);
    return sockfd;
}

pub fn close(fd: posix.socket_t) void {
    util.close(fd);
}

pub const ConnPassFn = *const fn (ptr: *anyopaque, conn: util.Connection) void;

pub const ConnectionPipe = struct {
    ptr: *anyopaque,
    submitFn: ConnPassFn,

    const Self = @This();

    pub fn submit(self: *Self, conn: util.Connection) void {
        self.submitFn(self.ptr, conn);
    }
};

pub fn handle(sockfd: posix.socket_t, _conn_pipe: ConnectionPipe) void {
    var conn_pipe = _conn_pipe;
    const log = std.log.scoped(.server_handle);

    while (true) {
        const conn = util.acceptConnection(sockfd) catch |err| {
            if (err == error.SocketNotListening) {
                // socket was closed
                log.debug("acceptConnectioned returned SocketNotListening a.k.a. EINVAL - socket closed", .{});
                break;
            }

            log.err("failed to accept connection: {}", .{err});
            continue;
        };

        log.debug("accepted connection from {}", .{conn.client_addr});
        conn_pipe.submit(conn);
    }
}

pub fn startThread(allocator: std.mem.Allocator, sockfd: posix.socket_t, conn_pipe: ConnectionPipe) !std.Thread {
    const thread = try std.Thread.spawn(.{ .allocator = allocator, .stack_size = 512 }, handle, .{ sockfd, conn_pipe });
    return thread;
}
