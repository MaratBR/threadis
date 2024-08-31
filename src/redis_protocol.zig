const std = @import("std");
const buf_util = @import("buf_util.zig");
const Allocator = std.mem.Allocator;
const AnyReader = std.io.AnyReader;
const AnyWriter = std.io.AnyWriter;

const STACK_BUF_SIZE = 4096;

pub const RedisReaderErr = error{ OutOfMemory, ReadError, RecursionDepth, UnexpectedChar, InvalidCRLF, IntegerTooBig, BufferTooBig, InvalidTypePrefix, InvalidDigit, InvalidEnum, InvalidStringType, BufferDidNotReadEnough };

pub const LastReaderErrorInfo = struct { read_error: ?anyerror };

pub const RedisReader = struct {
    pub const Error = RedisReaderErr;

    reader: AnyReader,

    peek: u8,

    allocator: Allocator,

    last_error_info: LastReaderErrorInfo,

    const Self = @This();

    pub fn init(allocator: Allocator, reader: AnyReader) Self {
        return .{
            // last read byte (for peek to work)
            .peek = 0,

            // reader instance that will abstract away buffering and other optimizations
            .reader = reader,

            .allocator = allocator,

            .last_error_info = .{ .read_error = null },
        };
    }

    inline fn peekByte(self: *Self) u8 {
        return self.peek;
    }

    inline fn readByte(self: *Self) RedisReaderErr!u8 {
        const b = self.reader.readByte() catch |err| {
            self.last_error_info.read_error = err;
            return RedisReaderErr.ReadError;
        };
        self.peek = b;
        return b;
    }

    inline fn read(self: *Self, buf: []u8) RedisReaderErr!usize {
        const read_bytes = self.reader.read(buf) catch |err| {
            self.last_error_info.read_error = err;
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
                return RedisReaderErr.BufferTooBig;
            }

            const b = try self.readByte();
            if (b == CR) {
                const b2 = try self.readByte();
                if (b2 == LF) {
                    return buf_builder.to_array();
                } else {
                    return RedisReaderErr.InvalidCRLF;
                }
            } else {
                try buf_builder.pushByte(b);
            }
        }

        const arr = try buf_builder.to_array();
        return arr;
    }

    inline fn discard(self: *Self, n: usize) RedisReaderErr!void {
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

    inline fn readCRLF(self: *Self) RedisReaderErr!void {
        var crlf: [2]u8 = undefined;
        _ = try self.read(&crlf);
        if (crlf[0] == CR and crlf[1] == LF) {
            return;
        } else {
            return RedisReaderErr.InvalidCRLF;
        }
    }

    inline fn discardCLRF(self: *Self) RedisReaderErr!void {
        _ = self.readCRLF();
    }

    inline fn discardUntilCLRF(self: *Self) RedisReaderErr!void {
        while (true) {
            const b = self.reader.readByte() catch |err| {
                self.last_error_info.read_error = err;
                return RedisReaderErr.ReadError;
            };
            if (b == CR) break;
        }
        const last_byte = try self.readByte();
        if (last_byte != LF) {
            return RedisReaderErr.InvalidCRLF;
        }
        return;
    }

    fn peekTypePrefix(self: *Self) RedisReaderErr!DataType {
        const b = self.peekByte();

        const typ = switch (b) {
            SIMPLE_STRING_PREFIX => DataType.SimpleString,
            STRING_PREFIX => DataType.String,
            ARRAY_PREFIX => DataType.Array,
            INT_PREFIX => DataType.Int,
            ERROR_PREFIX => DataType.Error,
            else => RedisReaderErr.InvalidTypePrefix,
        };

        return typ;
    }

    pub fn readTypePrefix(self: *Self) RedisReaderErr!DataType {
        _ = try self.readByte();
        const typ = try self.peekTypePrefix();
        return typ;
    }

    fn peekDigit(self: *Self) RedisReaderErr!u8 {
        const b = self.peekByte();
        if (b >= '0' and b <= '9') {
            return b - '0';
        }
        return RedisReaderErr.InvalidDigit;
    }

    pub fn readI64(self: *Self) RedisReaderErr!i64 {
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
                return RedisReaderErr.IntegerTooBig;
            }

            b = try self.peekDigit();
            v = v * 10 + b;
            iteration += 1;
            _ = try self.readByte();
        }

        std.debug.assert(self.peekByte() == CR);
        if (try self.readByte() != LF) {
            return RedisReaderErr.InvalidDigit;
        }

        if (is_negative) {
            v = -v;
        }

        return v;
    }

    pub fn readEnum(self: *Self, comptime T: type) RedisReaderErr!T {
        // const command_max_length = comptime getCommandMaxLength();
        const str = try self.readAnyString();
        if (str == null) {
            return RedisReaderErr.InvalidEnum;
        }
        _ = std.ascii.upperString(str.?, str.?);
        const value: ?T = std.meta.stringToEnum(T, str.?);
        std.debug.print("[RedisReader] ENUM (string): {s}\n", .{str.?});
        self.allocator.free(str.?);
        if (value == null) {
            return RedisReaderErr.InvalidEnum;
        } else {
            return value.?;
        }
    }

    pub fn readCommand(self: *Self) RedisReaderErr!Command {
        return self.readEnum(Command);
    }

    pub fn readClientSubCommand(self: *Self) RedisReaderErr!ClientCommand {
        return self.readEnum(ClientCommand);
    }

    pub fn readAnyString(self: *Self) RedisReaderErr!?[]u8 {
        const typ = try self.readTypePrefix();

        if (typ == .SimpleString) {
            return try self.readSimpleString();
        } else if (typ == .String) {
            return self.readString();
        } else {
            return RedisReaderErr.InvalidStringType;
        }
    }

    pub fn readString(self: *Self) RedisReaderErr!?[]u8 {
        const len = try self.readI64();
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
                return RedisReaderErr.BufferDidNotReadEnough;
            }
            return b;
        }
    }

    pub fn readSimpleString(self: *Self) RedisReaderErr![]u8 {
        const b = try self.readUntilCLRF(1024, 1024);
        return b;
    }

    pub fn readArrayHeader(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix != DataType.Array) {
            return RedisReaderErr.InvalidTypePrefix;
        }

        var len = try self.readI64();
        if (len < -1) len = -1;

        std.debug.print("[RedisReader] ARRAY: len={}\n", .{len});

        return len;
    }

    pub fn discardAnyValue(self: *Self) RedisReaderErr!DataType {
        return self.discardAnyValueWithRecursionDepth(0);
    }

    fn discardAnyValueWithRecursionDepth(self: *Self, recursion_depth: u8) RedisReaderErr!DataType {
        if (recursion_depth == 4) {
            return RedisReaderErr.RecursionDepth;
        }

        const typ = try self.readTypePrefix();

        switch (typ) {
            DataType.SimpleString => {
                try self.discardUntilCLRF();
            },
            DataType.String => {
                const len = try self.readI64();
                if (len > 0) {
                    try self.discard(@intCast(len));
                }
                try self.discard(2);
            },
            DataType.Int => {
                try self.discardUntilCLRF();
            },
            DataType.Error => {
                try self.discardUntilCLRF();
            },
            DataType.Array => {
                const arr_len = try self.readArrayHeader();
                if (arr_len > 0) {
                    for (0..@intCast(arr_len)) |_| {
                        _ = try self.discardAnyValueWithRecursionDepth(recursion_depth + 1);
                    }
                }
            },
        }

        return typ;
    }
};

pub const LastWriterErrorInfo = struct { write_error: ?anyerror = null };

pub const RedisWriterErr = error{WriteError};

pub const RedisWriter = struct {
    pub const Error = RedisWriterErr;

    allocator: Allocator,

    writer: AnyWriter,

    last_error: LastWriterErrorInfo = .{},

    const Self = @This();

    pub fn init(allocator: Allocator, writer: AnyWriter) Self {
        return .{ .allocator = allocator, .writer = writer };
    }

    pub fn writeI64(self: *Self, v: i64) RedisWriterErr!void {
        const max_len = 23;
        var buf: [max_len]u8 = undefined;
        buf[0] = ':';
        const numAsString = std.fmt.bufPrint(buf[1..], "{}", .{v}) catch {
            std.debug.panic("std.fmt.bufPrint failed", .{});
        };
        buf[numAsString.len] = CR;
        buf[numAsString.len + 1] = LF;

        _ = self.writer.write(buf[0 .. numAsString.len + 3]) catch |err| {
            std.debug.print("[writeI64] WriteError {}", .{err});
            self.last_error.write_error = err;
            return RedisWriterErr.WriteError;
        };
    }

    pub fn writeComptimeSimpleString(self: *Self, comptime str: []const u8) RedisWriterErr!void {
        var buf: [str.len + 3]u8 = undefined;
        buf[0] = SIMPLE_STRING_PREFIX;
        buf[str.len + 1] = CR;
        buf[str.len + 2] = LF;

        _ = self.writer.write(&buf) catch |err| {
            std.debug.print("[writeI64] WriteError {}", .{err});
            self.last_error.write_error = err;
            return RedisWriterErr.WriteError;
        };
    }
};
