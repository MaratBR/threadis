const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BinaryBuilder = struct {
    allocator: Allocator,
    buf: []u8,
    size: usize,
    buf_size: usize,

    const Self = @This();

    pub fn init(allocator: Allocator, buf_size: usize) Self {
        std.debug.assert(buf_size > 1024);

        return .{ .allocator = allocator, .buf = &.{}, .buf_size = buf_size, .size = 0 };
    }

    pub fn to_array(self: *Self) ![]u8 {
        if (self.size == 0) {
            return &.{};
        }

        if (self.buf.len == self.size) {
            const buf = self.buf;
            self.buf = &.{};
            self.size = 0;
            return buf;
        } else {
            const allocator = self.allocator;
            const buf = try allocator.alloc(u8, self.size);
            std.mem.copyForwards(u8, buf, self.buf);
            return buf;
        }
    }

    pub fn pushByte(self: *Self, byte: u8) Allocator.Error!void {
        if (self.size == 0) {
            const allocator = self.allocator;
            self.buf = try allocator.alloc(u8, self.buf_size);
        } else {
            try self.ensure_enough_space(self.size + 1);
            self.buf[self.size] = byte;
            self.size += 1;
        }
    }

    // pub fn push(self: *Self, buf: []const u8) !void {
    //     if (buf.len == 0) {
    //         return;
    //     }

    //     if (self.size == 0) {
    //         const required_buf_size = get_required_buf_size(self.buf_size, buf.len);
    //         const allocator = self.allocator;
    //         self.final_buf = try allocator.alloc(u8, required_buf_size);
    //     } else {
    //         self.ensure_enough_space(buf.len);
    //         std.mem.copyForwards(u8, self.final_buf[self.size..], buf);
    //     }
    // }

    fn ensure_enough_space(self: *Self, len: usize) Allocator.Error!void {
        if (self.size + len <= self.buf.len) {
            return;
        }

        const required_space = self.size + len;
        var new_buf_size = self.buf.len * 2;

        while (new_buf_size < required_space) {
            new_buf_size += self.buf_size;
        }

        const allocator = self.allocator;
        const new_buf = try allocator.alloc(u8, new_buf_size);
        std.mem.copyForwards(u8, new_buf, self.buf);
        allocator.free(self.buf);
        self.buf = new_buf;
    }

    pub fn deinit(self: *Self) void {
        if (self.buf.len > 0) {
            const allocator = self.allocator;
            allocator.free(self.buf);
        }
    }
};

inline fn get_required_buf_size(buf_size: usize, required_space: usize) usize {
    const bufs_count: f32 = std.math.ceil(@as(f32, @floatFromInt(required_space)) / @as(f32, @floatFromInt(buf_size)));
    return @as(usize, @intFromFloat(bufs_count)) * buf_size;
}
