const std = @import("std");
const Allocator = std.mem.Allocator;
const coro = @import("coro");

const ThreadPool = @import("thread_pool.zig").ThreadPool;

const Connection = std.net.Server.Connection;

const ClientRegistry = @import("client.zig").ClientRegistry;
const Client = @import("client.zig").Client;

const redis = @import("redis_protocol.zig");
const Command = redis.Command;
const ClientCommand = redis.ClientCommand;
const RedisReader = redis.RedisReader;
const RedisWriter = redis.RedisWriter;

allocator: Allocator,
thread_pool: ThreadPool,
worker_ctx: WorkerContext,

const Server = @This();

pub fn init(allocator: Allocator, thread_count: usize) Allocator.Error!Server {
    const thread_pool = ThreadPool.init(Allocator, thread_count);

    return .{
        // thread pool
        .thread_pool = thread_pool,

        // allocator
        .allocator = allocator,

        .worker_ctx = try WorkerContext.init(allocator),
    };
}

pub fn start(self: *Server) !void {
    self.thread_pool.start(threadLoop, .{&self.worker_ctx}) catch |err| {
        if (err != error.AlreadyStarted) {
            return err;
        }

        return;
    };

    return;
}

pub fn pushConnection(self: *Server, conn: Connection) Allocator.Error!void {
    try self.worker_ctx.queue.push(conn);
}

pub fn deinit(self: *ThreadPool) void {
    const allocator = self.allocator;
    allocator.free(self.threads);

    self.worker_ctx.deinit();
}

const WorkerContext = struct {
    // queue of connections
    queue: Queue(Connection),

    // client registry
    clients_registry: ClientRegistry,

    // allocator for worker
    allocator: Allocator,

    shared_mutex: std.Thread.RwLock = .{},

    const Self = @This();

    pub fn init(allocator: Allocator) Allocator.Error!Self {
        return .{
            .allocator = allocator,

            // client registry init
            .clients_registry = ClientRegistry.init(allocator),

            // thread-safe queue
            .queue = try Queue(Connection).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.queue.deinit();
        self.clients_registry.deinit();
    }
};

const InvalidCommandError = error{NotEnoughArguments};
const Error = union { reader_error: redis.RedisReaderErr, writer_error: redis.RedisWriterErr, invalid_command: InvalidCommandError };

var thread_counter: i64 = 0;

fn threadLoop(worker_context: *WorkerContext) void {
    var maybe_connection: ?Connection = null;

    worker_context.shared_mutex.lock();

    thread_counter += 1;
    const thread_id = thread_counter;
    std.debug.print("Worker {} started\n", .{thread_id});

    worker_context.shared_mutex.unlock();

    while (true) {
        maybe_connection = worker_context.queue.popWait(30_000);

        if (maybe_connection == null) {
            continue;
        }

        var connection = maybe_connection.?;
        defer connection.stream.close();
        const client = worker_context.clients_registry.registerConnection(connection.address) catch {
            std.time.sleep(20_000_000);
            continue;
        };

        const reader = connection.stream.reader().any();
        const writer = connection.stream.writer().any();

        const allocator = worker_context.allocator;
        var redis_reader = RedisReader.init(allocator, reader);
        var redis_writer = RedisWriter.init(allocator, writer);

        var conn_ctx = ConnectionContext{ .connection = &connection, .redis_writer = &redis_writer, .redis_reader = &redis_reader, .client = client };
        defer conn_ctx.deinit();

        var err: ?Error = null;

        while (true) {
            const array_header = redis_reader.readArrayHeader() catch |e| {
                std.debug.print("[command] read error when calling readArrayHeader {}\n", .{e});
                err = .{ .reader_error = e };
                break;
            };

            if (array_header <= 0) {
                // should never happen
                continue;
            }

            const command = redis_reader.readCommand() catch |e| {
                std.debug.print("[command] read error when calling readCommand {}\n", .{e});
                err = .{ .reader_error = e };
                break;
            };

            conn_ctx.command_arguments = @intCast(array_header - 1);
            conn_ctx.command = command;

            const maybe_handle = getCommandFunction(command);

            if (maybe_handle == null) {
                std.debug.print("[command] Cannot find handle for command {}\n", .{command});

                // disvard command variables
                for (0..conn_ctx.command_arguments) |_| {
                    _ = conn_ctx.redis_reader.discardAnyValue() catch |e| {
                        err = .{ .reader_error = e };
                        break;
                    };
                }
            } else {
                std.debug.print("[command] Executing handle for command {}\n", .{command});
                const handle = maybe_handle.?;
                handle(worker_context, &conn_ctx, &err);
                if (err != null) {
                    std.debug.print("[command] Detected and error after executing handle for {}\n", .{command});
                    break;
                }
            }

            conn_ctx.command_arguments = 0;
            conn_ctx.command = null;
        }

        std.debug.print("Error occured while handling command {any}: {}\n", .{ conn_ctx.command, err.? });
    }
}

const CommandError = redis.RedisReaderErr || redis.RedisWriterErr;

const ConnectionContext = struct {
    // redis protocol reader
    redis_reader: *RedisReader,

    // redis protocol writer
    redis_writer: *RedisWriter,

    // connection ptr
    connection: *Connection,

    // client ptr
    client: *Client,

    command_arguments: usize = 0,

    command: ?Command = null,

    const Self = @This();

    pub fn deinit(_: *Self) void {}
};

const CommandHandlerFn = *const fn (worker_ctx: *WorkerContext, conn_ctx: *ConnectionContext, err: *?Error) void;

fn getCommandFunction(command: Command) ?CommandHandlerFn {
    return switch (command) {
        Command.CLIENT => &commandClient,
        Command.PING => &commandPing,
        else => null,
    };
}

fn commandPing(_: *WorkerContext, conn_ctx: *ConnectionContext, err: *?Error) void {
    conn_ctx.redis_writer.writeComptimeSimpleString("PONG") catch |e| {
        err.* = .{ .writer_error = e };
    };
}

fn commandClient(_: *WorkerContext, conn_ctx: *ConnectionContext, err: *?Error) void {
    if (conn_ctx.command_arguments == 0) {
        err.* = .{ .invalid_command = InvalidCommandError.NotEnoughArguments };
        return;
    }

    const sub_command = conn_ctx.redis_reader.readClientSubCommand() catch |e| {
        if (e == RedisReader.Error.InvalidEnum) {
            std.debug.print("[command:CLIENT] invalid subcommand received\n", .{});
        } else {
            std.debug.print("[command:CLIENT] reader_error={}\n", .{e});
            err.* = .{ .reader_error = e };
        }
        return;
    };

    switch (sub_command) {
        ClientCommand.ID => {
            conn_ctx.redis_writer.writeI64(conn_ctx.client.id) catch |e| {
                std.debug.print("[command:CLIENT] writer_error={}", .{e});
                err.* = .{ .writer_error = e };
                return;
            };
            std.debug.print("[command:CLIENT] CLIENT ID to {}", .{conn_ctx.connection.address});
        },

        else => {
            for (0..conn_ctx.command_arguments - 1) |_| {
                _ = conn_ctx.redis_reader.discardAnyValue() catch |e| {
                    err.* = .{ .reader_error = e };
                    return;
                };
            }
            return;
        },
    }
}
