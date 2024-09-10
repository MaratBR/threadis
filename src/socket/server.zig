const std = @import("std");
const coro = @import("coro");

const posix = std.posix;
const util = @import("./util.zig");

pub const Options = util.Options;
pub const ConnPassFn = *const fn (ptr: *anyopaque, conn: util.Connection) void;

pub const ConnectionPipe = struct {
    ptr: *anyopaque,
    submitFn: ConnPassFn,

    const Self = @This();

    pub fn submit(self: *Self, conn: util.Connection) void {
        self.submitFn(self.ptr, conn);
    }
};

const acceptor_stack_size = 262144;

// pub fn startThread(allocator: std.mem.Allocator, sockfd: posix.socket_t, conn_pipe: ConnectionPipe) !std.Thread {
//     const thread = try std.Thread.spawn(.{ .allocator = allocator, .stack_size = acceptor_stack_size }, StdPosixAcceptor.handle, .{ sockfd, conn_pipe });
//     return thread;
// }

pub fn server(options: Options, _conn_pipe: ConnectionPipe) !void {
    const log = std.log.scoped(.server);
    log.debug("opening socket on {}...", .{options.addr});

    const sockfd = try util.create(options);
    defer util.close(sockfd);

    var conn_pipe = _conn_pipe;

    while (true) {
        log.debug("waiting for incoming connection...", .{});

        const conn = util.accept(sockfd) catch |err| {
            if (err == error.SocketNotListening) {
                // socket was closed
                log.debug("accepted returned SocketNotListening a.k.a. EINVAL - socket closed", .{});
                break;
            }

            log.err("failed to accept connection: {}", .{err});
            continue;
        };

        log.debug("accepted connection from {}", .{conn.addr});
        conn_pipe.submit(conn);
    }
}
