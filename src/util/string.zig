const std = @import("std");
const Allocator = std.mem.Allocator;

const String = @This();

buf: []u8,
allocator: Allocator,

pub fn initOwned(buf: []u8, allocator: Allocator) String {
    return .{ .buf = buf, .allocator = allocator };
}

pub fn initOwnedNullable(buf: ?[]u8, allocator: Allocator) ?String {
    if (buf == null) {
        return null;
    }
    return String.initOwned(buf.?, allocator);
}

pub fn deinit(self: String) void {
    self.allocator.free(self.buf);
}
