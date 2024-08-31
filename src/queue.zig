const std = @import("std");
const Allocator = std.mem.Allocator;
const Deque = @import("deque.zig").Deque;

pub fn Queue(comptime T: type) type {
    return struct {
        deque: Deque(T),
        mutex: std.Thread.Mutex,
        has_item: std.Thread.Condition,

        const Self = @This();

        pub fn init(allocator: Allocator) Allocator.Error!Self {
            return .{ .mutex = .{}, .has_item = .{}, .deque = try Deque(T).init(allocator) };
        }

        pub fn deinit(self: Self) void {
            self.deque.deinit();
        }

        pub fn pop(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();
            const v: ?T = self.deque.popFront();
            return v;
        }

        pub fn popWait(self: *Self, timeout_ms: u64) ?T {
            self.mutex.lock();
            var v: ?T = self.deque.popFront();

            if (v != null) {
                self.mutex.unlock();
                return v;
            }

            // timedWait will release mutex
            self.has_item.timedWait(&self.mutex, timeout_ms * 1_000_000) catch |err| {
                std.debug.assert(err == error.Timeout);
            };

            v = self.deque.popFront();
            self.mutex.unlock();
            return v;
        }

        pub fn push(self: *Self, v: T) Allocator.Error!void {
            self.mutex.lock();
            defer self.mutex.unlock();

            try self.deque.pushBack(v);
            self.has_item.signal();
        }
    };
}
