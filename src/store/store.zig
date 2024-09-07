const std = @import("std");
const Allocator = std.mem.Allocator;
const in_memory = @import("./in_memory_store.zig");

pub const Store = in_memory.InMemoryStore;
pub const Entry = in_memory.Entry;

pub fn createStore(allocator: Allocator) !Store {
    return Store.init(allocator, .{ .segments_count = 16 });
}
