const std = @import("std");
const Allocator = std.mem.Allocator;

const Connection = @import("../socket/util.zig").Connection;
const Arc = @import("../lib/zigrc.zig").Arc;
const Client = @import("../client.zig");
const Store = @import("../store/store.zig").Store;

const String = @import("../util/string.zig");
const buf_util = @import("../buf_util.zig");
const ctmap = @import("ctmap.zig");

pub const redis = @import("../redis.zig");

pub const CommandHandlerFn = *const fn (ctx: *Context) anyerror!void;

pub const CommandError = redis.RedisReaderErr || redis.RedisWriterErr;

pub const Context = struct {
    _redis_reader: *redis.RedisReader,
    redis_writer: *redis.RedisWriter,
    client: Client.Rc,
    command_arguments: usize = 0,
    read_command_arguments: usize = 0,
    command: []const u8,
    store: *Store,
    allocator: Allocator,
    conn_address: std.net.Address,

    const Self = @This();

    pub fn initUndefined(
        allocator: Allocator,
        client: Client.Rc,
        redis_reader: *redis.RedisReader,
        redis_writer: *redis.RedisWriter,
        store: *Store,
        conn_address: std.net.Address,
    ) Self {
        return .{ .conn_address = conn_address, .allocator = allocator, .client = client, .redis_writer = redis_writer, ._redis_reader = redis_reader, .store = store, .command = &.{} };
    }

    pub fn prepare(self: *Self, command: []const u8, arguments_count: usize) void {
        self.command = command;
        self.read_command_arguments = 0;
        self.command_arguments = arguments_count;
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

    pub fn beginSubcommand(self: *Self) !void {
        self.command_arguments -= self.read_command_arguments;
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

    pub const CommandHeader = struct {
        command: []const u8,
        arguments_count: u8,
        pub fn deinit(self: @This(), allocator: Allocator) void {
            allocator.free(self.command);
        }
    };

    pub fn readCommandHeader(self: *Self) !CommandHeader {
        const arr_size = try self._redis_reader.readArrayHeader();
        if (arr_size == 0) return error.InvalidCommandHeader;
        const maybe_command = try self._redis_reader.readString();
        if (maybe_command == null) return error.InvalidCommandHeader;
        const command = maybe_command.?;
        _ = std.ascii.lowerString(command, command);
        self.read_command_arguments = 0;
        return .{ .command = command, .arguments_count = @intCast(arr_size - 1) };
    }

    pub fn readI64(self: *Self) !i64 {
        std.debug.assert(self.command_arguments > self.read_command_arguments);
        const v = try self._redis_reader.readI64String();
        self.read_command_arguments += 1;
        return v;
    }

    fn writeArgNumErr(self: *Self) !void {
        var bb = buf_util.BinaryBuilder.init(self.allocator, 60);
        defer bb.deinit();
        try bb.push("wrong number of arguments for '");
        try bb.push(self.command);
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

        try self._redis_reader.discardNValues(self.command_arguments - self.read_command_arguments);
        self.read_command_arguments = self.command_arguments;
    }
};

pub const CommandDecl = struct {
    arity: comptime_int = 1,
    flags: CommandFlag = .{},
    handler: CommandHandlerFn,
    pos_first_key: comptime_int = 0,
    pos_last_key: comptime_int = 0,
    step_count_keys: comptime_int = 0,
};

pub const CommandInfo = struct {
    decl: CommandDecl,
    name: []const u8,

    const Self = @This();

    pub inline fn is(self: *const Self, name: []const u8) bool {
        if (name.len != self.name.len) return false;

        var lower: [self.name.len]u8 = undefined;
        inline for (0..self.name.len) |i| {
            lower[i] = std.ascii.toLower(name[i]);
        }

        return std.mem.eql(u8, lower[0..], self.name);
    }
};

pub fn CommandHandler(comptime command_decl: CommandInfo) type {
    return struct {
        const decl = command_decl.decl;
        pub const name = command_decl.name;

        pub fn handle(ctx: *Context) !void {
            return @This().decl.handler(ctx);
        }
    };
}

pub const CommandFlag = packed struct(u16) { write: bool = false, readonly: bool = false, denyoom: bool = false, admin: bool = false, pubsub: bool = false, noscript: bool = false, random: bool = false, sort_for_script: bool = false, loading: bool = false, stale: bool = false, skip_monitor: bool = false, asking: bool = false, fast: bool = false, movablekeys: bool = false, _padding: u2 = 0 };

fn comptimeToLower(comptime str: []const u8) []const u8 {
    var lower: [str.len]u8 = undefined;
    inline for (0..str.len) |i| {
        lower[i] = std.ascii.toLower(str[i]);
    }
    return &lower;
}

fn CreateCommandsHandler(comptime n: comptime_int, comptime subcommands: [n]type) type {
    var keys: [n][]const u8 = undefined;
    var values: [n]CommandHandlerFn = undefined;

    for (0..n) |i| {
        keys[i] = comptimeToLower(subcommands[i].name);
        values[i] = subcommands[i].handle;
    }

    const Map = ctmap.CTMap(n, keys, CommandHandlerFn, values);

    return struct {
        pub fn handle(ctx: *Context) !void {
            const handle_fn = Map.get(ctx.command) orelse {
                try ctx.redis_writer.writeError("unknown command");
                return;
            };

            try handle_fn(ctx);
        }
    };
}

pub fn Commands(comptime command: anytype) type {
    const command_type = @TypeOf(command);
    const type_info = @typeInfo(command_type);

    return switch (type_info) {
        .Struct => {
            const struct_info = type_info.Struct;
            if (struct_info.is_tuple) {
                @compileError("struct must not be a tuple");
            }

            if (struct_info.fields.len == 0) {
                @compileError("struct must not be empty");
            }

            var subcommands: [struct_info.fields.len]type = undefined;

            for (0..struct_info.fields.len) |i| {
                const field = struct_info.fields[i];
                const field_value = @field(command, field.name);

                if (@TypeOf(field_value) != type) {
                    @compileError("each field of the Commands struct must be a type");
                }

                if (!isHandlerType(field_value)) {
                    @compileError("each field of the Commands struct must be a handler");
                }

                subcommands[i] = field_value;
            }

            return CreateCommandsHandler(subcommands.len, subcommands);
        },
        else => {
            @compileError("Commands handler accepts struct of any type as an argument");
        },
    };
}

fn isHandlerType(comptime t: type) bool {
    return switch (@typeInfo(t)) {
        .Struct => {
            if (!std.meta.hasFn(t, "handle")) {
                return false;
            }

            // TODO more complex checks
            return true;
        },

        else => false,
    };
}

pub fn MkEnumReader(comptime map: anytype) type {
    const Map = ctmap.MkCTMap(map);
    return struct {
        pub inline fn read(ctx: *Context) anyerror!?Map.ValueType {
            const maybe_str = try ctx.readString();
            if (maybe_str == null) return null;
            const str = maybe_str.?;
            defer str.deinit();
            _ = std.ascii.lowerString(str.buf, str.buf);
            return Map.get(str.buf);
        }
    };
}

pub fn readCTEnum(comptime Map: type, ctx: *Context) anyerror!?Map.ValueType {
    const maybe_str = try ctx.readString();
    if (maybe_str == null) return null;
    const str = maybe_str.?;
    defer str.deinit();
    _ = std.ascii.lowerString(str.buf, str.buf);
    const value: ?Map.ValueType = Map.get(str.buf);
    return value;
}

pub fn readSubcommandHandler(ctx: *Context, comptime map: anytype) !?CommandHandlerFn {
    const Map = ctmap.MkCTMap(map);
    if (Map.ValueType != CommandHandlerFn) {
        @compileError("Map.ValueType must be CommandHandlerFn");
    }

    const maybe_value = try readCTEnum(Map, ctx);
    return maybe_value;
}
