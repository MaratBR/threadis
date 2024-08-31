const std = @import("std");
const Thread = std.Thread;
const Allocator = std.mem.Allocator;

threads: []Thread,
allocator: Allocator,
mutex: Thread.Mutex = .{},
started: bool = false,

const ThreadPool = @This();

pub fn init(allocator: Allocator, thread_count: usize) Allocator.Error!ThreadPool {
    std.debug.assert(thread_count > 0);
    const threads = try allocator.alloc(Thread, thread_count);
    return .{
        // threads array
        .threads = threads,

        // allocator
        .allocator = allocator,
    };
}

pub fn start(self: *ThreadPool, comptime function: anytype, args: anytype) !void {
    self.mutex.lock();
    defer self.mutex.unlock();

    if (self.started) {
        return error.AlreadyStarted;
    }

    const allocator = self.allocator;
    for (0..self.threads.len) |i| {
        self.threads[i] = try Thread.spawn(.{ .allocator = allocator }, function, args);
    }

    self.started = true;

    return;
}

pub fn deinit(self: *ThreadPool) void {
    const allocator = self.allocator;
    allocator.free(self.threads);
}
