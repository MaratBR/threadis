const std = @import("std");
const Allocator = std.mem.Allocator;

const coro = @import("coro");

const socket_util = @import("./socket/util.zig");
const ConnectionPipe = @import("./socket/server.zig").ConnectionPipe;

const redis_proto = @import("./redis/redis_protocol.zig");

const handlers = @import("./handlers/all_handlers.zig");

const Store = @import("./store/store.zig").Store;

const Client = @import("./client.zig");

const Arc = @import("./lib/zigrc.zig").Arc;

const log = std.log.scoped(.Handler);

const Handler = @This();

// allocator: std.mem.Allocator,
pool: coro.ThreadPool,
sched: coro.Scheduler,
allocator: std.mem.Allocator,
store: *Store,
client_registry: *Client.Registry,

pub fn init(allocator: std.mem.Allocator, store: *Store, client_registry: *Client.Registry) !Handler {
    const thread_pool = try coro.ThreadPool.init(allocator, .{ .max_threads = 16 });
    const sched = try coro.Scheduler.init(allocator, .{});

    return .{ .allocator = allocator, .pool = thread_pool, .sched = sched, .client_registry = client_registry, .store = store };
}

pub fn deinit(self: *Handler) void {
    self.pool.deinit();
    self.sched.deinit();
}

pub fn submitConnection(self: *Handler, conn: socket_util.Connection) void {
    const conn_data = blk: {
        const client = self.client_registry.registerConnection() catch |err| {
            std.debug.panic("failed to register new connection: {}", .{err});
        };

        const data = ConnectionData.init(conn, client, self.store);

        break :blk data;
    };

    _ = self.pool.spawnForCompletition(&self.sched, handleConnection, .{ self.allocator, conn_data }, .{ .allocator = self.allocator }) catch |err| {
        log.err("failed to spawn corutine for completion for client {}: {}", .{ conn.client_addr, err });
        conn.close() catch |close_err| {
            log.err("failed to close connection after failing to spawn handler corutine: {}. POTENTIAL MEMORY LEAK", .{close_err});
        };
    };
}

pub fn connPipe(self: *Handler) ConnectionPipe {
    return .{ .ptr = @ptrCast(self), .submitFn = typeErasedSubmitFn };
}

fn typeErasedSubmitFn(context: *anyopaque, conn: socket_util.Connection) void {
    const ptr: *Handler = @alignCast(@ptrCast(context));
    Handler.submitConnection(ptr, conn);
}

const ConnectionData = struct {
    conn: socket_util.Connection,
    store: *Store,
    client: Client.Rc,

    pub const Self = @This();

    pub fn init(conn: socket_util.Connection, client: Client.Rc, store: *Store) ConnectionData {
        return .{ .conn = conn, .client = client, .store = store };
    }

    pub fn deinit(self: *Self) void {
        self.client.release();
    }
};

fn handleConnection(allocator: std.mem.Allocator, conn_data: ConnectionData) !void {
    const log_handleConnection = std.log.scoped(.handleConnection);

    // create reader and writer for this connection
    var conn_reader = try conn_data.conn.reader(allocator);
    defer conn_reader.deinit();
    var conn_writer = try conn_data.conn.writer();
    defer conn_writer.deinit();
    var r = redis_proto.RedisReader.init(allocator, conn_reader.any());
    var w = redis_proto.RedisWriter.init(allocator, conn_writer.any());

    var command_ctx = handlers.Context.initUndefined(allocator, conn_data.client.retain(), &r, &w, conn_data.store, conn_data.conn.client_addr);
    defer command_ctx.deinit();

    var closed = false;

    while (true) {
        handleCommand(&command_ctx) catch |err| {
            if (err == error.ReadError) {
                std.debug.assert(r.last_error.read_error != null);
                const read_error = r.last_error.read_error.?;
                if (read_error == error.SocketNotConnected or read_error == error.ConnectionResetByPeer) {
                    // connection closed by peer
                    closed = true;
                    break;
                }
            } else if (err == error.WriteError) {
                std.debug.assert(w.last_error.write_error != null);
                const write_error = w.last_error.write_error.?;
                if (write_error == error.ConnectionResetByPeer) {
                    closed = true;
                    break;
                }
            }

            log_handleConnection.err("error encountered while processing command from {}", .{conn_data.conn.client_addr});
            return err;
        };
    }

    if (!closed) {
        conn_data.conn.close() catch |err| {
            std.debug.panic("failed to close connection with peer {}: {}", .{ conn_data.conn.client_addr, err });
        };
        log_handleConnection.debug("closed connection to {}", .{conn_data.conn.client_addr});
    } else {
        log_handleConnection.debug("closed connection to {} by peer", .{conn_data.conn.client_addr});
        // w.writePing()
    }
}

fn handleCommand(ctx: *handlers.Context) !void {
    const log_handleCommand = std.log.scoped(.handleCommand);
    const command_header = ctx.readCommandHeader() catch |err| {
        if (err == error.EmptyCommandHeader) {
            return;
        }

        return err;
    };

    const handler = handlers.getCommandHandler(command_header.command);

    log_handleCommand.debug("executing handler for {} command with {} arguments", .{ command_header.command, command_header.arguments_count });
    try handler(ctx);

    if (ctx.read_command_arguments != ctx.command_arguments) {
        std.debug.panic("command handler for {} did not read all arguments from the client stream but finished successfully", .{command_header.command});
    }
}
