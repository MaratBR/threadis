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

pub const DataType = enum {
    SimpleString,
    String,
    Error,
    Int,
    Array,
};

pub const ValueType = enum {
    String,
    Int,
};

pub const Value = struct { typ: DataType, val: union {
    string: []u8,
} };

const STACK_BUF_SIZE = 4096;

pub const RedisReaderErr = Allocator.Error || error{ ReadError, RecursionDepth, UnexpectedChar, InvalidCRLF, IntegerTooBig, InvalidInteger, BufferTooBig, InvalidTypePrefix, InvalidDigit, InvalidEnum, InvalidStringType, BufferDidNotReadEnough };

pub const LastReaderErrorInfo = struct { read_error: ?anyerror = null, parse_error: ?anyerror = null };

pub const RedisReader = struct {
    pub const Error = RedisReaderErr;

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

    inline fn read(self: *Self, buf: []u8) RedisReaderErr!usize {
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
                return RedisReaderErr.BufferTooBig;
            }

            const b = try self.readByte();
            if (b == CR) {
                const b2 = try self.readByte();
                if (b2 == LF) {
                    return buf_builder.collect();
                } else {
                    return RedisReaderErr.InvalidCRLF;
                }
            } else {
                try buf_builder.pushByte(b);
            }
        }

        const arr = try buf_builder.collect();
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
                self.last_error.read_error = err;
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

    inline fn peekTypePrefix(self: *Self) RedisReaderErr!DataType {
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

    inline fn peekDigit(self: *Self) RedisReaderErr!u8 {
        const b = self.peekByte();
        if (b >= '0' and b <= '9') {
            return b - '0';
        }
        return RedisReaderErr.InvalidDigit;
    }

    pub fn readI64(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix != .Int) {
            return RedisReaderErr.InvalidTypePrefix;
        }

        return try self.internalReadI64();
    }

    pub fn readI64String(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix == .Int) {
            return try self.internalReadI64();
        } else if (prefix == .SimpleString) {
            const s = try self.internalReadSimpleString();
            defer self.allocator.free(s);
            const i = std.fmt.parseInt(i64, s, 10) catch |err| {
                self.last_error.parse_error = err;
                return error.InvalidInteger;
            };
            return i;
        } else if (prefix == .String) {
            const s = try self.internalReadBulkString();
            if (s == null) {
                return error.InvalidInteger;
            }
            defer self.allocator.free(s.?);
            const i = std.fmt.parseInt(i64, s.?, 10) catch |err| {
                self.last_error.parse_error = err;
                return error.InvalidInteger;
            };
            return i;
        } else {
            return RedisReaderErr.InvalidTypePrefix;
        }
    }

    fn internalReadI64(self: *Self) RedisReaderErr!i64 {
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
        const str = try self.readString();
        if (str == null) {
            return RedisReaderErr.InvalidEnum;
        }
        _ = std.ascii.lowerString(str.?, str.?);
        const value: ?T = std.meta.stringToEnum(T, str.?);
        defer self.allocator.free(str.?);
        if (value == null) {
            std.log.err("invalid command enum: {s}", .{str.?});
            return RedisReaderErr.InvalidEnum;
        } else {
            return value.?;
        }
    }

    pub fn readString(self: *Self) RedisReaderErr!?[]u8 {
        const typ = try self.readTypePrefix();

        if (typ == .SimpleString) {
            return try self.internalReadSimpleString();
        } else if (typ == .String) {
            return self.internalReadBulkString();
        } else {
            return RedisReaderErr.InvalidStringType;
        }
    }

    fn internalReadBulkString(self: *Self) RedisReaderErr!?[]u8 {
        const len = try self.internalReadI64();
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

    fn internalReadSimpleString(self: *Self) RedisReaderErr![]u8 {
        const b = try self.readUntilCLRF(1024, 1024);
        return b;
    }

    pub fn readArrayHeader(self: *Self) RedisReaderErr!i64 {
        const prefix = try self.readTypePrefix();
        if (prefix != .Array) {
            return RedisReaderErr.InvalidTypePrefix;
        }

        var len = try self.internalReadI64();
        if (len < -1) len = -1;

        return len;
    }

    pub fn discardAnyValue(self: *Self) RedisReaderErr!DataType {
        return self.discardAnyValueWithRecursionDepth(0);
    }

    pub inline fn discardNValues(self: *Self, n: usize) RedisReaderErr!void {
        for (0..n) |_| {
            _ = try self.discardAnyValue();
        }
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
                const len = try self.internalReadI64();
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
    const log = std.log.scoped(.RedisWriter);
    pub const Error = RedisWriterErr;

    allocator: Allocator,

    writer: AnyWriter,

    last_error: LastWriterErrorInfo = .{},

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
        return self.writeInteger(i64, v);
    }

    pub inline fn writeUsize(self: *Self, v: usize) RedisWriterErr!void {
        return self.writeInteger(usize, v);
    }

    inline fn writeInteger(self: *Self, comptime TInt: type, v: TInt) RedisWriterErr!void {
        try self.writeByte(INT_PREFIX);
        try self.internalWriteInteger(TInt, v);
        try self.internalWriteCRLF();
    }

    inline fn internalWriteInteger(self: *Self, comptime TInt: type, v: TInt) RedisWriterErr!void {
        const max_len = (comptime maxIntStringSize(TInt)) + 1;
        var buf: [max_len]u8 = undefined;
        const numAsString = std.fmt.bufPrint(&buf, "{}", .{v}) catch {
            std.debug.panic("std.fmt.bufPrint failed", .{});
        };

        return self.write(buf[0..numAsString.len]);
    }

    // fn internalWriteI64(self: *Self, v: i64) RedisWriterErr!void {
    //     return self.internalWriteInteger(i64, v);
    //     // const max_len = 20;
    //     // var buf: [max_len]u8 = undefined;
    //     // buf[0] = ':';
    //     // const numAsString = std.fmt.bufPrint(&buf, "{}", .{v}) catch {
    //     //     std.debug.panic("std.fmt.bufPrint failed", .{});
    //     // };

    //     // return self.write(buf[0..numAsString.len]);
    // }

    pub inline fn writeEmptyArray(self: *Self) RedisWriterErr!void {
        try self.writeArrayHeader(0);
    }

    pub fn writeArrayHeader(self: *Self, v: i64) RedisWriterErr!void {
        try self.writeByte(ARRAY_PREFIX);
        try self.internalWriteInteger(i64, v);
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
        try self.writeByte(ERROR_PREFIX);
        try self.write(err);
        try self.internalWriteCRLF();
    }

    pub fn writeBulkString(self: *Self, v: []const u8) RedisWriterErr!void {
        std.debug.assert(v.len <= 524288000); // 500 Mebibytes is max size for a redis bulk string
        _ = self.writer.writeByte(STRING_PREFIX) catch |err| {
            log.err("[writeI64] WriteError {}", .{err});
            self.last_error.write_error = err;
            return RedisWriterErr.WriteError;
        };
        try self.internalWriteInteger(usize, v.len);
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

fn maxIntStringSize(comptime TInt: type) comptime_int {
    var len: usize = 0;

    const type_info = @typeInfo(TInt);

    switch (type_info) {
        .Int => {
            if (type_info.Int.signedness == .signed) {
                len += 1;
            }
        },
        else => {
            @compileError("maxIntStringSize expects type argument to be an integer of some kind");
        },
    }

    var v: usize = std.math.maxInt(TInt);

    while (v != 0) {
        len += 1;
        v /= 10;
    }

    return len;
}
