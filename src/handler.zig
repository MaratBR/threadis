const std = @import("std");
const Allocator = std.mem.Allocator;

const coro = @import("coro");

const socket_util = @import("./socket/util.zig");
const ConnectionPipe = @import("./socket/server.zig").ConnectionPipe;

const redis = @import("./redis.zig");

const handlers = @import("./handlers/all_handlers.zig");

const Store = @import("./store/store.zig").Store;

const Client = @import("./client.zig");

const Arc = @import("./lib/zigrc.zig").Arc;

const log = std.log.scoped(.handler);

const Handler = @This();

allocator: std.mem.Allocator,
store: *Store,
client_registry: *Client.Registry,

sched: *coro.Scheduler,
thread_pool: *coro.ThreadPool,

pub fn init(allocator: std.mem.Allocator, store: *Store, client_registry: *Client.Registry, sched: *coro.Scheduler, thread_pool: *coro.ThreadPool) !Handler {
    return .{ .allocator = allocator, .thread_pool = thread_pool, .sched = sched, .client_registry = client_registry, .store = store };
}

pub fn deinit(_: *Handler) void {}

pub fn submitConnection(self: *Handler, conn: socket_util.Connection) void {
    const conn_data = blk: {
        const client = self.client_registry.registerConnection() catch |err| {
            std.debug.panic("failed to register new connection: {}", .{err});
        };

        const data = ConnectionData.init(conn, client, self.store);

        break :blk data;
    };

    log.debug("passing connection {} to the thread pool", .{conn.addr});

    _ = self.sched.spawn(handleConnection, .{ self.allocator, conn_data }, .{}) catch |err| {
        log.err("failed to spawn corutine for completion for client {}: {}", .{ conn.addr, err });
        conn.close() catch |close_err| {
            log.err("failed to close connection after failing to spawn handler corutine: {}. POTENTIAL MEMORY LEAK", .{close_err});
        };
    };

    // _ = self.thread_pool.spawnForCompletition(self.sched, handleConnection, .{ self.allocator, conn_data }, .{ .allocator = self.allocator }) catch |err| {
    //     log.err("failed to spawn corutine for completion for client {}: {}", .{ conn.addr, err });
    //     conn.close() catch |close_err| {
    //         log.err("failed to close connection after failing to spawn handler corutine: {}. POTENTIAL MEMORY LEAK", .{close_err});
    //     };
    // };
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

    log_handleConnection.debug("preparing connection {}...", .{conn_data.conn.addr});

    // create reader and writer for this connection
    var conn_reader = try conn_data.conn.reader(allocator);
    defer conn_reader.deinit();
    var conn_writer = try conn_data.conn.writer();
    defer conn_writer.deinit();
    var r = redis.RedisReader.init(allocator, conn_reader.any());
    var w = redis.RedisWriter.init(allocator, conn_writer.any());

    var command_ctx = handlers.Context.initUndefined(allocator, conn_data.client.retain(), &r, &w, conn_data.store, conn_data.conn.addr);
    defer command_ctx.deinit();

    var closed = false;
    var received_quit = false;

    log_handleConnection.debug("waiting for commands from {}...", .{conn_data.conn.addr});

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
                if (write_error == error.ConnectionResetByPeer or write_error == error.BrokenPipe) {
                    closed = true;
                    break;
                }
            } else if (err == error.Quit) {
                // connection termination is requested
                received_quit = true;
                break;
            }

            log_handleConnection.err("error encountered while processing command from {}: {}", .{ conn_data.conn.addr, err });
            break;
        };
    }

    if (received_quit) {
        log_handleConnection.debug("received quit command from peer {}", .{conn_data.conn.addr});
    }

    if (!closed) {
        conn_data.conn.close() catch |err| {
            std.debug.panic("failed to close connection with peer {}: {}", .{ conn_data.conn.addr, err });
        };
        log_handleConnection.debug("closed connection to {}", .{conn_data.conn.addr});
    } else {
        log_handleConnection.debug("closed connection to {} by peer", .{conn_data.conn.addr});
    }
}

fn handleCommand(ctx: *handlers.Context) !void {
    const log_handleCommand = std.log.scoped(.handler_handleCommand);

    const command_header = try ctx.readCommandHeader();
    defer command_header.deinit(ctx.allocator);
    const handler = handlers.getCommandHandler(command_header.command);
    ctx.prepare(command_header.command, command_header.arguments_count);

    log_handleCommand.debug("executing handler for {s} command with {} arguments", .{ command_header.command, command_header.arguments_count });
    const started_at = now();
    try handler(ctx);
    const elapsed = now().since(started_at);
    if (elapsed > std.time.ns_per_ms * 10) {
        log_handleCommand.debug("finished executing handler for {s} command in {}ms", .{ command_header.command, elapsed / std.time.ns_per_ms });
    }

    if (ctx.read_command_arguments != ctx.command_arguments) {
        log_handleCommand.err("command handler for {s} did not read all arguments from the client stream but finished successfully, read_command_arguments={}, command_arguments={}", .{ command_header.command, ctx.read_command_arguments, ctx.command_arguments });
        try ctx.discardRemainingArguments();
    }
}

fn now() std.time.Instant {
    const instant = std.time.Instant.now() catch |err| {
        std.debug.panic("failed to get Instant: {}", .{err});
    };
    return instant;
}

fn isUserError(err: anyerror) bool {
    return err == error.IntegerTooBig or err == error.NotEnoughArguments or err == error.InvalidEnum;
}
