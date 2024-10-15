const std = @import("std");
const Allocator = std.mem.Allocator;
const AnyReader = std.io.AnyReader;
const AnyWriter = std.io.AnyWriter;

const buf_util = @import("buf_util.zig");
const Entry = @import("store/store.zig").Entry;

pub const SIMPLE_STRING_PREFIX = '+';
pub const STRING_PREFIX = '$';
pub const ARRAY_PREFIX = '*';
pub const ERROR_PREFIX = '-';
pub const INT_PREFIX = ':';

pub const LF = '\n';
pub const CR = '\r';
pub const CRLF = "\r\n";

pub const TypePrefixEnum = enum(u8) {
    simple_string = SIMPLE_STRING_PREFIX,
    string = STRING_PREFIX,
    array = ARRAY_PREFIX,
    int = INT_PREFIX,
};

pub const ReadableValueType = enum {
    string,
    int,
    array_header,
};

pub const ReadableValue = union(ReadableValueType) {
    string: ?[]const u8,
    int: i64,
    array_header: i64,
};

const STACK_BUF_SIZE = 4096;

pub const RedisReaderErr = Allocator.Error || error{
    // IO error during read operation
    //
    ReadError,

    // Invalid protocol format
    //
    ProtocolError,

    // Protocol format is correct but passed value is not processable,
    // for example string integer that is over the int64 limit or a
    // string that way too big
    //
    InvalidValue,

    // Various interal errors
    //
    RecursionLimitExceeded,

    // Parameters errors
    //
    InvalidParameters,
};

pub const LastReaderErrorInfo = struct {
    read_error: ?anyerror = null,
    msg: ?*const [:0]u8 = null,
    source: ?*const [:0]u8 = null,
};

pub const RedisReader = struct {
    pub const Error = RedisReaderErr;
    const log = std.log.scoped(.RedisReader);

    reader: AnyReader,
    peek: u8,
    allocator: Allocator,
    last_error: LastReaderErrorInfo,

    const Self = @This();

    pub fn init(allocator: Allocator, reader: AnyReader) Self {
        return .{
            // last read byte (for peek to work)
            .peek = 0,

            // reader instance that will abstract away buffering and other optimizations
            .reader = reader,

            .allocator = allocator,

            .last_error = .{},
        };
    }

    inline fn peekByte(self: *Self) u8 {
        return self.peek;
    }

    inline fn readByte(self: *Self) RedisReaderErr!u8 {
        const b = self.reader.readByte() catch |err| {
            self.last_error.read_error = err;
            return RedisReaderErr.ReadError;
        };
        self.peek = b;
        return b;
    }

    fn read(self: *Self, buf: []u8) RedisReaderErr!usize {
        const read_bytes = self.reader.read(buf) catch |err| {
            self.last_error.read_error = err;
            return RedisReaderErr.ReadError;
        };
        const last_byte = buf[read_bytes - 1];
        self.peek = last_byte;
        return read_bytes;
    }

    fn readUntilCLRF(self: *Self, limit: usize, buffer_size: usize) RedisReaderErr![]u8 {
        const allocator = self.allocator;
        var buf_builder = buf_util.BinaryBuilder.init(allocator, buffer_size);
        defer buf_builder.deinit();

        while (true) {
            if (buf_builder.size >= limit) {
                try self.discardUntilCLRF();
                self.last_error.msg = "reached max size of the buffer for storing the value";
                self.last_error.source = "readUntilCLRF";
                return RedisReaderErr.InvalidValue;
            }

            const b = try self.readByte();
            if (b == CR) {
                const b2 = try self.readByte();
                if (b2 == LF) {
                    return buf_builder.collect();
                } else {
                    self.last_error.msg = "expected LF after CR, but got something else instead";
                    self.last_error.source = "readUntilCLRF";
                    return RedisReaderErr.ProtocolError;
                }
            } else {
                try buf_builder.pushByte(b);
            }
        }

        const arr = try buf_builder.collect();
        return arr;
    }

    fn discard(self: *Self, n: usize) RedisReaderErr!void {
        var trash: [STACK_BUF_SIZE]u8 = undefined;
        var remaining = n;

        while (remaining > 0) {
            if (remaining > trash.len) {
                _ = try self.read(&trash);
                remaining -= trash.len;
            } else {
                const slice = trash[0..remaining];
                _ = try self.read(slice);
                std.debug.assert(slice.len == remaining);
                remaining = 0;
            }
        }
    }

    fn readCRLF(self: *Self) RedisReaderErr!void {
        var crlf: [2]u8 = undefined;
        _ = try self.read(&crlf);
        if (crlf[0] == CR and crlf[1] == LF) {
            return;
        } else {
            self.last_error.msg = "expected LF after CR, but got something else instead";
            self.last_error.source = "readUntilCLRF";
            return RedisReaderErr.ProtocolError;
        }
    }

    inline fn discardCLRF(self: *Self) RedisReaderErr!void {
        _ = self.readCRLF();
    }

    fn discardUntilCLRF(self: *Self) RedisReaderErr!void {
        while (true) {
            const b = self.reader.readByte() catch |err| {
                self.last_error.read_error = err;
                return RedisReaderErr.ReadError;
            };
            if (b == CR) break;
        }
        const last_byte = try self.readByte();
        if (last_byte != LF) {
            self.last_error.msg = "expected LF after CR, but got something else instead";
            self.last_error.source = "discardUntilCLRF";
            return RedisReaderErr.ProtocolError;
        }
        return;
    }

    inline fn peekTypePrefix(self: *Self) RedisReaderErr!TypePrefixEnum {
        const b = self.peekByte();

        const typ = switch (b) {
            SIMPLE_STRING_PREFIX => TypePrefixEnum.simple_string,
            STRING_PREFIX => TypePrefixEnum.string,
            ARRAY_PREFIX => TypePrefixEnum.array,
            INT_PREFIX => TypePrefixEnum.int,
            ERROR_PREFIX => {
                Self.log.debug("received error as input", .{});
                self.last_error.msg = "invalid type prefix: error cannot be unsed as an input type";
                self.last_error.source = "peekTypePrefix";
                return RedisReaderErr.ProtocolError;
            },
            else => {
                Self.log.debug("invalid type prefix: {c}", .{b});
                self.last_error.msg = "invalid type prefix";
                self.last_error.source = "peekTypePrefix";
                return RedisReaderErr.ProtocolError;
            },
        };

        return typ;
    }

    pub fn readTypePrefix(self: *Self) RedisReaderErr!TypePrefixEnum {
        _ = try self.readByte();
        const typ = try self.peekTypePrefix();
        return typ;
    }

    pub fn readValue(self: *Self) RedisReaderErr!ReadableValue {
        const typ = try self.readTypePrefix();
        return switch (typ) {
            .simple_string => {
                const str = try self.readSimpleStringAssumePrefix();
                if (str.len > 0) {
                    const int = std.fmt.parseInt(i64, str, 10) catch {
                        return .{ .string = str };
                    };
                    return .{ .int = int };
                } else {
                    return .{ .string = str };
                }
            },
            .string => {
                const maybe_str = try self.readBulkStringAssumePrefix();
                if (maybe_str) |str| {
                    const int = std.fmt.parseInt(i64, str, 10) catch {
                        return .{ .string = str };
                    };
                    return .{ .int = int };
                } else {
                    return .{ .string = null };
                }
            },
            .array => ReadableValue{ .array_header = try self.readArrayHeaderAssumePrefix() },
            .int => ReadableValue{ .int = try self.readI64AssumePrefix() },
        };
    }

    // reads i64 value
    pub fn readI64(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix != .int) {
            try self.discardTypeWithRecursionDepth(prefix, 4);
            self.last_error.msg = "expected int, but got something else instead";
            self.last_error.source = "readI64";
            return RedisReaderErr.InvalidValue;
        }

        return try self.readI64AssumePrefix();
    }

    // reads an integer or a string representation of an integer,
    // if string is not a valid i64, will return an error
    pub fn readI64String(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix == .int) {
            return try self.readI64AssumePrefix();
        } else if (prefix == .simple_string) {
            const s = try self.readSimpleStringAssumePrefix();
            defer self.allocator.free(s);
            const i = std.fmt.parseInt(i64, s, 10) catch {
                self.last_error.msg = "invalid integer format";
                self.last_error.source = "readI64String";
                return RedisReaderErr.InvalidValue;
            };
            return i;
        } else if (prefix == .string) {
            const s = try self.readBulkStringAssumePrefix();
            if (s == null) {
                self.last_error.msg = "invalid integer format: string is null";
                self.last_error.source = "readI64String";
                return RedisReaderErr.InvalidValue;
            }
            defer self.allocator.free(s.?);
            const i = std.fmt.parseInt(i64, s.?, 10) catch {
                self.last_error.msg = "invalid integer format";
                self.last_error.source = "readI64String";
                return RedisReaderErr.InvalidValue;
            };
            return i;
        } else {
            self.last_error.msg = "expected int or string representation, but got something else instead";
            self.last_error.source = "readI64String";
            return RedisReaderErr.InvalidValue;
        }
    }

    fn readI64AssumePrefix(self: *Self) RedisReaderErr!i64 {
        var v: i64 = 0;
        var is_negative = false;

        var b = try self.readByte();

        if (b == '-') {
            is_negative = true;
        } else if (b == '+') {
            // ignore first byte  and just read next byte since + sign is redundant
            _ = try self.readByte();
        }

        b = try self.peekDigit();
        v = b;
        var iteration: u8 = 0;
        _ = try self.readByte();

        while (self.peekByte() != CR) {
            if (iteration >= 18) {
                // reached max number of digits
                try self.discardUntilCLRF();

                self.last_error.msg = "int is outside of int64 range";
                self.last_error.source = "readI64AssumePrefix";
                return RedisReaderErr.InvalidValue;
            }

            b = try self.peekDigit();
            // TODO properly check overflow
            v = v * 10 + b;
            iteration += 1;
            _ = try self.readByte();
        }

        std.debug.assert(self.peekByte() == CR);
        if (try self.readByte() != LF) {
            self.last_error.msg = "expected LF after CR, but got something else instead";
            self.last_error.source = "readI64AssumePrefix";
            return RedisReaderErr.ProtocolError;
        }

        if (is_negative) {
            v = -v;
        }

        return v;
    }

    inline fn peekDigit(self: *Self) RedisReaderErr!u8 {
        const b = self.peekByte();
        if (b >= '0' and b <= '9') {
            return b - '0';
        }

        // NOTE recoverable error
        self.last_error.msg = "expected a digit 0-9, but got something else instead";
        self.last_error.source = "peekDigit";
        return RedisReaderErr.ProtocolError;
    }

    pub fn readEnum(self: *Self, comptime T: type) RedisReaderErr!T {
        // const command_max_length = comptime getCommandMaxLength();
        const str = try self.readString();
        if (str == null) {
            self.last_error.msg = "expected string representation of " ++ @typeName(T) ++ " enum but got null";
            self.last_error.source = "readEnum";
            return RedisReaderErr.InvalidValue;
        }
        _ = std.ascii.lowerString(str.?, str.?);
        const value: ?T = std.meta.stringToEnum(T, str.?);
        defer self.allocator.free(str.?);
        if (value == null) {
            std.log.err("invalid command enum: {s}", .{str.?});
            self.last_error.msg = "expected string representation of " ++ @typeName(T) ++ " enum but got invalid value";
            self.last_error.source = "readEnum";
            return RedisReaderErr.InvalidValue;
        } else {
            return value.?;
        }
    }

    pub fn readString(self: *Self) RedisReaderErr!?[]u8 {
        const typ = try self.readTypePrefix();

        if (typ == .simple_string) {
            return try self.readSimpleStringAssumePrefix();
        } else if (typ == .string) {
            return self.readBulkStringAssumePrefix();
        } else {
            self.last_error.msg = "expected string value, but got something else instead";
            self.last_error.source = "readString";
            return RedisReaderErr.InvalidValue;
        }
    }

    fn readBulkStringAssumePrefix(self: *Self) RedisReaderErr!?[]u8 {
        const len = try self.readI64AssumePrefix();
        if (len < 0) {
            return null; // null string
        } else if (len == 0) {
            try self.readCRLF();
            return &.{};
        } else {
            const allocator = self.allocator;
            std.debug.assert(len > 0);
            const b = try allocator.alloc(u8, @as(usize, @intCast(len)));
            const bytes_read = try self.read(b);

            try self.readCRLF();

            if (bytes_read < b.len) {
                self.last_error.msg = "while reading bulk string did not entire length of the buffer";
                self.last_error.source = "readBulkStringAssumePrefix";
                return RedisReaderErr.ProtocolError;
            }
            return b;
        }
    }

    fn readSimpleStringAssumePrefix(self: *Self) RedisReaderErr![]u8 {
        const b = try self.readUntilCLRF(1024, 1024);
        return b;
    }

    pub fn readArrayHeader(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix != .array) {
            self.last_error.msg = @ptrCast(@alignCast("expected array, but got something else instead"));
            self.last_error.source = @ptrCast(@alignCast("readArrayHeader"));
            return RedisReaderErr.InvalidValue;
        }

        return try self.readArrayHeaderAssumePrefix();
    }

    fn readArrayHeaderAssumePrefix(self: *Self) RedisReaderErr!i64 {
        var len = try self.readI64AssumePrefix();
        if (len < -1) len = -1;

        return len;
    }

    pub inline fn discardNValues(self: *Self, n: usize) RedisReaderErr!void {
        for (0..n) |_| {
            _ = try self.discardValue();
        }
    }

    pub fn discardValue(self: *Self) RedisReaderErr!TypePrefixEnum {
        return self.discardWithRecursionDepth(4);
    }

    fn discardWithRecursionDepth(self: *Self, recursion_depth: u8) RedisReaderErr!TypePrefixEnum {
        const typ = try self.readTypePrefix();

        try self.discardTypeWithRecursionDepth(typ, recursion_depth);
        return typ;
    }

    fn discardTypeWithRecursionDepth(self: *Self, typ: TypePrefixEnum, recursion_depth: u8) RedisReaderErr!void {
        if (recursion_depth == 0) {
            return RedisReaderErr.RecursionLimitExceeded;
        }

        switch (typ) {
            TypePrefixEnum.simple_string => {
                try self.discardUntilCLRF();
            },
            TypePrefixEnum.string => {
                const len = try self.readI64AssumePrefix();
                if (len > 0) {
                    try self.discard(@intCast(len));
                }
                try self.discard(2);
            },
            TypePrefixEnum.int => {
                try self.discardUntilCLRF();
            },
            TypePrefixEnum.array => {
                const arr_len = try self.readArrayHeader();
                if (arr_len > 0) {
                    for (0..@intCast(arr_len)) |_| {
                        _ = try self.discardWithRecursionDepth(recursion_depth - 1);
                    }
                }
            },
        }
    }

    pub fn readParameters(
        self: *Self,
        max_arguments: usize,
        comptime pos_t: type,
        comptime f_t: type,
    ) RedisReaderErr!Parameters(pos_t, f_t) {
        const pos_ti = @typeInfo(pos_t);
        const f_ti = @typeInfo(f_t);

        if (f_ti != .Struct) {
            @compileError("f_t must be a struct");
        }
        if (pos_ti != .Struct) {
            @compileError("pos_t must be a struct");
        }

        const pos_tis = pos_ti.Struct;
        const f_tis = f_ti.Struct;

        // validate flags type
        if (f_tis.is_tuple) {
            @compileError("pos_t cannot be a tuple struct");
        }
        inline for (f_tis.fields) |field| {
            const ti = @typeInfo(field.type);
            if (ti != .Optional) {
                @compileError("all flags must be optional");
            }
            const t = ti.Optional.child;
            if (t != i64 and t != []const u8 and t != bool) {
                @compileError("all flags must be i64 or []const u8");
            }
        }

        // validate positional args
        comptime var optional = false;
        comptime var required_args: usize = 0;
        inline for (pos_tis.fields) |field| {
            comptime var t = field.type;
            comptime var ti = @typeInfo(t);

            if (ti == .Optional) {
                optional = true;
                t = ti.Optional.child;
                ti = @typeInfo(t);
            } else {
                if (optional) {
                    @compileError("required positional argument cannot follow non-optional one");
                }
                required_args += 1;
            }

            if (t != i64 and t != []const u8) {
                @compileError("each field of positional must be []const u8 or i64");
            }
        }

        var pos: pos_t = comptime getDefaultValue(pos_t);
        var flags: f_t = comptime getDefaultValue(f_t);
        var flag_name: ?[]const u8 = null;
        var read_args: usize = 0;

        inline for (0..pos_tis.fields.len) |i| {
            const field = pos_tis.fields[i];

            if (read == max_arguments) {
                if (i < required_args) {
                    return RedisReaderErr.InvalidParameters;
                } else {
                    break;
                }
            }

            read_args += 1;

            if (i < required_args) {
                if (field.type == []const u8) {
                    @field(pos, field.name) = try self.readString();
                } else if (field.type == i64) {
                    @field(pos, field.name) = try self.readI64String();
                } else {
                    unreachable;
                }
            } else {
                if (field.type == []const u8) {
                    const maybe_str = try self.readString();
                    if (maybe_str) |str| {
                        if (isFlag(f_t, str)) {
                            // start reading flags instead
                            flag_name = str;
                        } else {
                            // just a string
                            @field(pos, field.name) = str;
                        }
                    } else {
                        break;
                    }
                } else {
                    const value = try self.readValue();

                    if (value == .int) {
                        @field(pos, field.name) = value.int;
                    } else if (value == .string) {
                        const maybe_str = value.string;
                        if (maybe_str) |str| {
                            if (isFlag(f_t, str)) {
                                // start reading flags instead
                                flag_name = str;
                                break;
                            } else {
                                // just a string which is not valid in this case
                                return RedisReaderErr.InvalidParameters;
                            }
                        }
                    } else if (value == .array_header) {
                        return RedisReaderErr.InvalidParameters;
                    } else {
                        unreachable;
                    }
                }
            }
        }

        var remaining_flags: usize = f_tis.fields.len;

        outer: while (remaining_flags > 0) {
            if (flag_name == null) {
                if (read_args == max_arguments) {
                    break :outer;
                }

                flag_name = try self.readString();
                if (flag_name == null) {
                    break;
                }
            }

            const f = flag_name.?;

            inline for (f_tis.fields) |field| {
                const t = @typeInfo(field.type).Optional.child;
                if (std.ascii.eqlIgnoreCase(field.name, f)) {
                    remaining_flags -= 1;
                    if (t == i64) {
                        if (read == max_arguments) {
                            return ReadParametersError.NotEnoughParameters;
                        }
                        @field(flags, field.name) = try self.readI64String();
                        read_args += 1;
                    } else if (t == []const u8) {
                        if (read == max_arguments) {
                            return ReadParametersError.NotEnoughParameters;
                        }
                        @field(flags, field.name) = try self.readString();
                        read_args += 1;
                    } else if (t == bool) {
                        @field(flags, field.name) = true;
                    } else {
                        unreachable;
                    }
                    break;
                }
            }
        }

        return Parameters(pos_t, f_t).init(self.allocator, pos, flags, read);
    }
};

pub const LastWriterErrorInfo = struct { write_error: ?anyerror = null };

pub const RedisWriterErr = error{WriteError};

pub const RedisWriter = struct {
    const log = std.log.scoped(.RedisWriter);
    pub const Error = RedisWriterErr;

    allocator: Allocator,
    writer: AnyWriter,
    last_error: LastWriterErrorInfo = .{},
    written_something: bool = false,

    const Self = @This();

    pub fn init(allocator: Allocator, writer: AnyWriter) Self {
        return .{ .allocator = allocator, .writer = writer };
    }

    fn write(self: *Self, v: []const u8) RedisWriterErr!void {
        _ = self.writer.write(v) catch |err| {
            log.err("[writeI64] WriteError {}", .{err});
            self.last_error.write_error = err;
            return RedisWriterErr.WriteError;
        };
    }

    fn writeByte(self: *Self, v: u8) RedisWriterErr!void {
        var buf: [1]u8 = undefined;
        buf[0] = v;
        return self.write(&buf);
    }

    fn internalWriteCRLF(self: *Self) RedisWriterErr!void {
        return self.write(CRLF);
    }

    pub inline fn writeI64(self: *Self, v: i64) RedisWriterErr!void {
        return self.writeInteger(v);
    }

    pub inline fn writeUsize(self: *Self, v: usize) RedisWriterErr!void {
        return self.writeInteger(v);
    }

    inline fn writeInteger(self: *Self, v: anytype) RedisWriterErr!void {
        self.written_something = true;
        try self.writeByte(INT_PREFIX);
        try self.internalWriteInteger(v);
        try self.internalWriteCRLF();
    }

    inline fn internalWriteInteger(self: *Self, v: anytype) RedisWriterErr!void {
        const max_len = (comptime maxIntStringSize(if (@TypeOf(v) == comptime_int) v else @TypeOf(v))) + 1;
        var buf: [max_len]u8 = undefined;
        const numAsString = std.fmt.bufPrint(&buf, "{}", .{v}) catch {
            std.debug.panic("std.fmt.bufPrint failed", .{});
        };

        return self.write(buf[0..numAsString.len]);
    }

    pub inline fn writeEmptyArray(self: *Self) RedisWriterErr!void {
        try self.writeArrayHeader(0);
    }

    pub fn writeArrayHeader(self: *Self, v: anytype) RedisWriterErr!void {
        self.written_something = true;
        try self.writeByte(ARRAY_PREFIX);
        try self.internalWriteInteger(v);
        try self.internalWriteCRLF();
    }

    pub fn writeValue(self: *Self, v: *const Entry.Value) RedisWriterErr!void {
        switch (v.type) {
            .binary => {
                try self.writeBulkString(v.raw.binary.buf);
            },
            .i64 => {
                try self.writeI64(v.raw.i64);
            },
        }
    }

    pub fn writeError(self: *Self, err: []const u8) RedisWriterErr!void {
        self.written_something = true;
        try self.writeByte(ERROR_PREFIX);
        try self.write(err);
        try self.internalWriteCRLF();
    }

    pub fn writeBulkString(self: *Self, v: []const u8) RedisWriterErr!void {
        self.written_something = true;
        std.debug.assert(v.len <= 524288000); // 500 Mebibytes is max size for a redis bulk string
        _ = self.writer.writeByte(STRING_PREFIX) catch |err| {
            log.err("[writeI64] WriteError {}", .{err});
            self.last_error.write_error = err;
            return RedisWriterErr.WriteError;
        };
        try self.internalWriteInteger(v.len);
        try self.internalWriteCRLF();

        if (v.len > 0) {
            _ = self.writer.write(v) catch |err| {
                log.err("[writeI64] WriteError {}", .{err});
                self.last_error.write_error = err;
                return RedisWriterErr.WriteError;
            };
        }

        try self.internalWriteCRLF();
    }

    pub fn writeNull(self: *Self) RedisWriterErr!void {
        return self.write("$-1\r\n");
    }

    pub inline fn writeComptimeSimpleString(self: *Self, comptime str: []const u8) RedisWriterErr!void {
        const buf = comptime getSimpleStringBuf(str);

        return self.write(&buf);
    }

    pub inline fn writeSimpleString(self: *Self, str: []const u8) RedisWriterErr!void {
        try self.writeByte(SIMPLE_STRING_PREFIX);
        try self.write(str);
        try self.internalWriteCRLF();
    }

    pub inline fn writeOK(self: *Self) RedisWriterErr!void {
        return self.writeComptimeSimpleString("OK");
    }

    pub inline fn writeBulkNullString(self: *Self) RedisWriterErr!void {
        var null_bulk_string: [5]u8 = undefined;
        null_bulk_string[0] = STRING_PREFIX;
        null_bulk_string[1] = '-';
        null_bulk_string[2] = '1';
        null_bulk_string[3] = CR;
        null_bulk_string[4] = LF;
        try self.write(&null_bulk_string);
    }
};

fn getSimpleStringBuf(comptime str: []const u8) [str.len + 3]u8 {
    var buf: [str.len + 3]u8 = undefined;
    buf[0] = SIMPLE_STRING_PREFIX;
    buf[str.len + 1] = CR;
    buf[str.len + 2] = LF;
    for (0..str.len) |i| {
        buf[1 + i] = str[i];
    }

    return buf;
}

fn maxIntStringSize(comptime TInt: anytype) comptime_int {
    var len: usize = 0;

    if (@TypeOf(TInt) == comptime_int) {
        var v = TInt;
        if (v < 0) {
            len += 1;
            v = -v;
        }
        while (v != 0) {
            len += 1;
            v /= 10;
        }
        return len;
    } else if (@TypeOf(TInt) != type) {
        @compileError("maxIntStringSize expects type argument to be a comptime_int or a type of integer (u64, i32 etc), got: " ++ @typeName(TInt));
    }

    const type_info = @typeInfo(TInt);

    switch (type_info) {
        .Int => {
            if (type_info.Int.signedness == .signed) {
                len += 1;
            }
        },
        else => {
            @compileError("maxIntStringSize expects type argument to be an integer of some kind, got: " ++ @typeName(TInt));
        },
    }

    var v = std.math.maxInt(TInt);

    while (v != 0) {
        len += 1;
        v /= 10;
    }

    return len;
}

pub fn Parameters(
    comptime pos_t: type,
    comptime f_t: type,
) type {
    const f_tis = @typeInfo(f_t).Struct;

    return struct {
        positional_args: pos_t,
        flags: f_t,
        allocator: Allocator,
        arguments_read: usize,

        pub fn init(
            allocator: Allocator,
            positional_args: pos_t,
            flags: f_t,
            arguments_read: usize,
        ) @This() {
            return .{ .flags = flags, .positional_args = positional_args, .allocator = allocator, .arguments_read = arguments_read };
        }

        pub fn deinit(self: @This()) void {
            inline for (f_tis.fields) |field| {
                if (field.type == ?[]const u8) {
                    if (@field(self.flags, field.name)) |v| {
                        self.allocator.free(v);
                    }
                } else if (field.type == ?i64) {} else {
                    comptime unreachable;
                }
            }
        }
    };
}

pub const ReadParametersError = RedisReaderErr;

fn getDefaultValue(comptime T: type) T {
    const type_info = @typeInfo(T);

    return switch (type_info) {
        .Int => 0,
        .Null => null,
        .Float => 0.0,
        .ComptimeInt => 0,
        .Bool => false,
        .Optional => null,
        .Struct => {
            var v: T = undefined;
            for (type_info.Struct.fields) |field| {
                if (field.default_value != null) {
                    @field(v, field.name) = field.default_value.?;
                } else {
                    @field(v, field.name) = getDefaultValue(field.type);
                }
            }
            return v;
        },
        else => {
            @compileError("cannot calculate default value for type" ++ @typeName(T));
        },
    };
}

fn isFlag(comptime T: type, name: []const u8) bool {
    inline for (@typeInfo(T).Struct.fields) |field| {
        if (std.ascii.eqlIgnoreCase(field.name, name)) {
            return true;
        }
    }

    return false;
}
