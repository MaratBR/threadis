const std = @import("std");
const Allocator = std.mem.Allocator;

const UnmanagedEntryValue = @This();

type: Type,
raw: EntryUntypedValue,

pub fn initI64(v: i64) UnmanagedEntryValue {
    return .{ .type = Type.i64, .raw = .{ .i64 = v } };
}

pub fn initBinary(v: []u8) UnmanagedEntryValue {
    return .{ .type = Type.binary, .raw = .{ .binary = UnmanagedBuffer.init(v) } };
}

pub fn convertToString(self: *UnmanagedEntryValue, allocator: Allocator) Allocator.Error!void {
    if (self.type == .binary) return;

    switch (self.type) {
        .i64 => {
            const max_len = 20;
            var buf: [max_len]u8 = undefined;
            const numAsString = std.fmt.bufPrint(&buf, "{}", .{self.raw.i64}) catch {
                std.debug.panic("failed to convert int to string", .{});
            };

            self.deinit(allocator);
            self.* = try UnmanagedEntryValue.initBinary(numAsString).copy(allocator);
        },
        else => unreachable,
    }
}

pub fn copy(self: *const UnmanagedEntryValue, allocator: Allocator) Allocator.Error!UnmanagedEntryValue {
    switch (self.type) {
        .binary => {
            const buf_copy = try allocator.dupe(u8, self.raw.binary.buf);
            return UnmanagedEntryValue.initBinary(buf_copy);
        },
        .i64 => {
            return UnmanagedEntryValue.initI64(self.raw.i64);
        },
    }
}

pub inline fn bytesLen(self: *const UnmanagedEntryValue) usize {
    return switch (self.type) {
        .binary => self.raw.binary.buf.len,
        .i64 => 4,
    };
}

pub fn deinit(self: UnmanagedEntryValue, allocator: Allocator) void {
    switch (self.type) {
        Type.binary => {
            self.raw.binary.deinit(allocator);
        },
        else => {},
    }
}

pub const EntryUntypedValue = union { i64: i64, binary: UnmanagedBuffer };

pub const Type = enum { i64, binary };

pub const UnmanagedBuffer = struct {
    buf: []u8,

    const Self = @This();

    pub fn init(buf: []u8) Self {
        return .{ .buf = buf };
    }

    pub fn copy(self: Self, allocator: Allocator) Allocator.Error!Self {
        const buf = try allocator.dupe(u8, self.buf);
        return .{ .buf = buf };
    }

    pub fn append(self: *Self, allocator: Allocator, buf: []const u8) Allocator.Error!void {
        if (buf.len == 0) return;

        self.buf = try allocator.realloc(self.buf, self.buf.len + buf.len);
        std.mem.copyForwards(u8, self.buf[self.buf.len - buf.len ..], buf);
    }

    pub fn deinit(self: Self, allocator: Allocator) void {
        allocator.free(self.buf);
    }
};
