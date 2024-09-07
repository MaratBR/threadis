const std = @import("std");
const Allocator = std.mem.Allocator;

const redis_proto = @import("../redis/redis_protocol.zig");
const RedisReader = redis_proto.RedisReader;
const RedisWriter = redis_proto.RedisWriter;
const Connection = @import("../socket/util.zig").Connection;
const Arc = @import("../lib/zigrc.zig").Arc;
const Client = @import("../client.zig");
const Store = @import("../store/store.zig").Store;

const String = @import("../util/string.zig");
const buf_util = @import("../buf_util.zig");

pub const redis = @import("../redis/redis_def.zig");

pub const CommandHandlerFn = *const fn (ctx: *Context) anyerror!void;

pub const CommandError = redis.RedisReaderErr || redis.RedisWriterErr;

pub const Context = struct {
    _redis_reader: *RedisReader,
    redis_writer: *RedisWriter,
    client: Client.Rc,
    command_arguments: usize = 0,
    read_command_arguments: usize = 0,
    command: redis.Command,
    store: *Store,
    allocator: Allocator,
    conn_address: std.net.Address,

    const Self = @This();

    pub fn initUndefined(
        allocator: Allocator,
        client: Client.Rc,
        redis_reader: *RedisReader,
        redis_writer: *RedisWriter,
        store: *Store,
        conn_address: std.net.Address,
    ) Self {
        return .{ .conn_address = conn_address, .allocator = allocator, .client = client, .redis_writer = redis_writer, ._redis_reader = redis_reader, .store = store, .command = undefined };
    }

    pub fn prepare(self: *Self, command: redis.Command, arguments_count: usize) void {
        self.command = command;
        self.read_command_arguments = 0;
        self.command_arguments = arguments_count;
    }

    pub const ReadCommandHeaderError = RedisReader.Error || error{EmptyCommandHeader};

    pub fn readCommandHeader(self: *Self) ReadCommandHeaderError!struct { command: redis.Command, arguments_count: usize } {
        const arguments_count = try self._redis_reader.readArrayHeader();
        if (arguments_count <= 0) {
            return ReadCommandHeaderError.EmptyCommandHeader;
        }

        const command = try self._redis_reader.readCommand();

        self.prepare(command, @intCast(arguments_count - 1));

        return .{ .command = command, .arguments_count = @intCast(arguments_count - 1) };
    }

    pub fn deinit(self: *Self) void {
        self.client.release();
    }

    pub fn minArgNum(self: *Self, n: usize) !bool {
        if (self.command_arguments < n) {
            try self.writeArgNumErr();
            try self.discardRemainingArguments();
            return false;
        }
        return true;
    }

    pub fn maxArgNum(self: *Self, n: usize) !bool {
        if (self.command_arguments > n) {
            try self.writeArgNumErr();
            try self.discardRemainingArguments();
            return false;
        }
        return true;
    }

    pub fn exactArgNum(self: *Self, n: usize) !bool {
        if (self.command_arguments != n) {
            try self.writeArgNumErr();
            try self.discardRemainingArguments();
            return false;
        }
        return true;
    }

    pub fn readString(self: *Self) !?String {
        std.debug.assert(self.command_arguments > self.read_command_arguments);
        const v = try self._redis_reader.readString();
        self.read_command_arguments += 1;
        return String.initOwnedNullable(v, self._redis_reader.allocator);
    }

    pub fn readEnum(self: *Self, comptime T: type) !?T {
        std.debug.assert(self.command_arguments > self.read_command_arguments);
        const v = try self._redis_reader.readEnum(T);
        self.read_command_arguments += 1;
        return v;
    }

    pub fn readI64(self: *Self) !i64 {
        std.debug.assert(self.command_arguments > self.read_command_arguments);
        const v = try self._redis_reader.readI64();
        self.read_command_arguments += 1;
        return v;
    }

    fn writeArgNumErr(self: *Self) !void {
        var bb = buf_util.BinaryBuilder.init(self.allocator, 60);
        defer bb.deinit();
        try bb.push("wrong number of arguments for '");
        try bb.push(@tagName(self.command));
        try bb.push("' command");
        const err = try bb.collect();
        defer self.allocator.free(err);
        try self.redis_writer.writeError(err);
    }

    pub fn discardRemainingArguments(self: *Self) !void {
        std.debug.assert(self.read_command_arguments <= self.command_arguments);
        if (self.read_command_arguments == self.command_arguments) {
            return;
        }

        if (self.command_arguments - self.read_command_arguments == 0) return;

        try self._redis_reader.discardNValues(self.command_arguments - self.read_command_arguments);
    }
};
