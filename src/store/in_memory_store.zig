const std = @import("std");

const UnmanagedEntryValue = @import("./value.zig");
const glob = @import("../util/glob.zig");

const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;

const log = std.log.scoped(.in_memory_store);

pub const Entry = struct {
    value: UnmanagedEntryValue,
    allocator: Allocator,
    // in-memory store itself has ref to this entry
    _rc: u16 = 1,
    _rw: std.Thread.RwLock = .{},

    const Self = @This();
    pub const Value = UnmanagedEntryValue;

    pub fn initAsCopy(allocator: Allocator, value: *const UnmanagedEntryValue) Allocator.Error!Self {
        const valueCopy = try value.copy(allocator);

        return .{ .value = valueCopy, .allocator = allocator };
    }

    pub fn isBorrowed(self: *Self) bool {
        return @atomicLoad(u16, &self._rc, .monotonic) > 0;
    }

    pub fn borrow(self: *Self) Borrowed {
        std.debug.assert(@atomicLoad(u16, &self._rc, .monotonic) < std.math.maxInt(u16));
        _ = @atomicRmw(u16, &self._rc, .Add, 1, .monotonic);
        return .{ .entry = self, ._released = false };
    }

    pub fn release(self: *Self) void {
        if (@atomicRmw(u16, &self._rc, .Sub, 1, .monotonic) != 1) {
            return;
        }

        self.deinit();
        return;
    }

    pub fn set(self: *Self, value: *const UnmanagedEntryValue) Allocator.Error!void {
        const valueCopy = try value.copy(self.allocator);
        self._rw.lock();
        defer self._rw.unlock();
        self.value.deinit(self.allocator);
        self.value = valueCopy;
    }

    pub fn append(self: *Self, append_buf: []const u8) Allocator.Error!void {
        self._rw.lock();
        defer self._rw.unlock();

        try self.value.convertToString(self.allocator);
        try self.value.raw.binary.append(self.allocator, append_buf);
    }

    pub inline fn len(self: *const Self) usize {
        return self.value.bytesLen();
    }

    pub fn deinit(self: *Self) void {
        std.debug.assert(!self.isBorrowed());
        self.value.deinit(self.allocator);
    }

    pub const Borrowed = struct {
        entry: *Entry,
        _released: bool,

        pub fn release(self: *Borrowed) void {
            std.debug.assert(!self._released);

            @atomicStore(bool, &self._released, true, .monotonic);
            self.entry.release();
        }
    };
};

pub const KeyIterFunction = *const fn (key: []const u8) anyerror!void;

const Segment = struct {
    // max size of this segment in bytes
    max_size: usize,
    // lookup table containing all values store in the segment
    lookup_table: StringHashMap(*Entry),
    // mutex for accesing the values stored
    mutex: std.Thread.RwLock,
    count: usize,
    allocator: Allocator,
    id: u16,

    const Self = @This();

    pub fn init(allocator: Allocator, id: u16) Self {
        return .{

            // max size of this segment in bytes (WIP)
            .max_size = 100,
            .allocator = allocator,
            // mutex for reads and writes to the segment
            .mutex = .{},
            // in memory hash map
            .lookup_table = StringHashMap(*Entry).init(allocator),
            // atomic counter
            .count = 0,
            .id = id,
        };
    }

    pub const ScanIterator = struct {
        segment: *Segment,
        key_iterator: StringHashMap(*Entry).KeyIterator,

        // how many keys are left to be read
        remaining: usize,

        // number of values read
        read: usize = 0,

        // cursor or in simpler terms - offset in the lookup table
        cursor: u32,
        next_cursor: u32 = 0,
        initialized: u1 = 0,
        skip_dirty: u1,
        pattern: []const u8,

        pub fn init(cursor: u32, remaining: usize, segment: *Segment, skip_dirty: bool, pattern: []const u8) @This() {
            return .{ .key_iterator = segment.lookup_table.keyIterator(), .segment = segment, .cursor = cursor, .remaining = remaining, .skip_dirty = if (skip_dirty) 1 else 0, .pattern = pattern };
        }

        pub fn next(self: *ScanIterator) ?[]const u8 {
            if (self.remaining == 0) {
                return null;
            }

            if (self.initialized == 0) {
                self.initialized = 1;
                self.segment.lock();
                if (self.skip_dirty == 1) {
                    if (self.key_iterator.len < self.cursor) {
                        // cursor is more than ther size of the lookup table
                        self.remaining = 0;
                        self.next_cursor = 0;
                        return null;
                    } else {
                        self.key_iterator.len -= self.cursor;
                        self.key_iterator.items += self.cursor;
                    }
                } else {
                    for (0..self.cursor) |_| {
                        if (self.key_iterator.next() == null) {
                            self.remaining = 0;
                            return null;
                        }
                    }
                }
            }

            const key_ptr = self.key_iterator.next();

            if (key_ptr == null) {
                self.remaining = 0;
                return null;
            } else {
                self.read += 1;
                self.remaining -= 1;
                return key_ptr.?.*;
            }
        }

        pub fn deinit(self: *@This()) void {
            if (self.initialized == 1) {
                self.segment.unlock();
            }
        }
    };

    pub fn scan(self: *Self, cursor: u32, count: usize, pattern: []const u8) Self.ScanIterator {
        const it = Self.ScanIterator.init(cursor, count, self, true, pattern);
        return it;
    }

    pub fn lock(self: *Self) void {
        self.mutex.lock();
        // log.debug("segment #{} locked", .{self.id});
    }

    pub fn unlock(self: *Self) void {
        self.mutex.unlock();
        // log.debug("segment #{} unlocked", .{self.id});
    }

    pub fn get(self: *Self, key: []const u8) ?Entry.Borrowed {
        std.debug.assert(key.len > 0);

        return self.getInternal(key);
    }

    pub fn put(self: *Self, key: []const u8, value: *const UnmanagedEntryValue) Allocator.Error!void {
        std.debug.assert(key.len > 0);

        const entry_ptr = try self.allocator.create(Entry);
        errdefer self.allocator.destroy(entry_ptr);
        entry_ptr.* = try Entry.initAsCopy(self.allocator, value);
        const key_copy = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(key_copy);

        self.mutex.lock();
        defer self.mutex.unlock();
        try self.lookup_table.put(key_copy, entry_ptr); // OutOfMemory
    }

    pub fn del(self: *Self, key: []const u8) void {
        std.debug.assert(key.len > 0);

        self.mutex.lock();
        defer self.mutex.unlock();
        const ptr = self.lookup_table.getPtr(key) orelse return;
        _ = self.lookup_table.remove(key);
        _ = ptr.*.release();
    }

    fn getInternal(self: *Self, key: []const u8) ?Entry.Borrowed {
        self.mutex.lockShared();
        const entry_opt = self.lookup_table.get(key);
        const entry: *Entry = entry_opt orelse {
            self.mutex.unlockShared();
            return null;
        };

        const entry_borrowed = entry.borrow();
        self.mutex.unlockShared();
        return entry_borrowed;
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        var value_iter = self.lookup_table.valueIterator();
        while (value_iter.next()) |value| {
            value.*.release();
        }
        self.mutex.unlock();

        self.lookup_table.deinit();
    }
};

pub const InMemoryStoreConfig = struct {
    segments_count: u8,
};

pub const InMemoryStore = struct {
    segments: []Segment,
    allocator: Allocator,
    segment_mask: usize,

    const Self = @This();

    pub const Value = UnmanagedEntryValue;

    pub fn init(allocator: Allocator, config: InMemoryStoreConfig) Allocator.Error!Self {
        const segment_count = config.segments_count;
        std.debug.assert(std.math.isPowerOfTwo(segment_count));

        const segments = try allocator.alloc(Segment, segment_count);
        for (0..segment_count) |i| {
            segments[i] = Segment.init(allocator, @intCast(i));
        }

        return .{ .segments = segments, .allocator = allocator, .segment_mask = segment_count - 1 };
    }

    pub fn get(self: *Self, key: []const u8) ?Entry.Borrowed {
        return self.getSegment(key).get(key);
    }

    pub fn put(self: *Self, key: []const u8, value: *const UnmanagedEntryValue) Allocator.Error!void {
        try self.getSegment(key).put(key, value);
    }

    pub fn del(self: *Self, key: []const u8) void {
        self.getSegment(key).del(key);
    }

    fn getSegment(self: *Self, key: []const u8) *Segment {
        const hash: u64 = std.hash.Wyhash.hash(0, key);
        return &self.segments[self.segment_mask & hash];
    }

    pub const ScanIterator = struct {
        store: *Self,
        remaining: usize,
        read: usize = 0,
        segment_idx: u16,
        pattern: []const u8,
        segment_cursor: u32,
        seg_iterator: ?Segment.ScanIterator = null,

        pub fn init(store: *Self, c: i64, count: usize, pattern: []const u8) @This() {
            const cursor_cast = @as(u48, @intCast(c));
            const segment_idx = @as(u16, @intCast((cursor_cast >> 32) & 0xffff));
            const segment_cursor: u32 = @intCast(cursor_cast & ~@as(u48, 0xffff << 32));

            return .{ .store = store, .segment_idx = segment_idx, .segment_cursor = segment_cursor, .remaining = count, .pattern = pattern };
        }

        pub fn cursor(self: *const @This()) i64 {
            if (self.seg_iterator) |seg_iterator| {
                const v: u48 = @as(u48, @intCast(self.segment_idx)) << 32 | seg_iterator.cursor;
                return @intCast(v);
            } else {
                return 0;
            }
        }

        pub fn nextSegment(self: *@This()) ?*Segment.ScanIterator {
            if (self.remaining == 0) {
                return null;
            }

            if (self.seg_iterator != null) {
                var seg_iterator = &self.seg_iterator.?;
                std.debug.assert(seg_iterator.remaining == 0);
                std.debug.assert(self.remaining >= seg_iterator.read);
                self.read += seg_iterator.read;
                self.remaining -= seg_iterator.read;
                self.segment_cursor = 0;
                seg_iterator.deinit();
                self.seg_iterator = null;
            }

            if (self.segment_idx >= self.store.segments.len) {
                // reached the end of the store
                self.remaining = 0;
                return null;
            }

            const segment = &self.store.segments[self.segment_idx];
            self.seg_iterator = segment.scan(self.segment_cursor, self.remaining, self.pattern);
            std.debug.assert(self.segment_idx != std.math.maxInt(@TypeOf(self.segment_idx)));
            self.segment_idx += 1;

            return &self.seg_iterator.?;
        }

        pub fn deinit(self: *@This()) void {
            if (self.seg_iterator != null) {
                self.seg_iterator.?.deinit();
                self.seg_iterator = null;
            }
        }
    };

    pub fn scan(self: *Self, cursor: i64, count: usize, pattern: []const u8) Self.ScanIterator {
        return Self.ScanIterator.init(self, cursor, count, pattern);
    }

    pub fn deinit(self: *Self) void {
        const allocator = self.allocator;
        for (self.segments) |*segment| {
            segment.deinit();
        }
        allocator.free(self.segments);
    }
};

// const Cursor = packed struct {
//     segment_idx: u16,
//     segment_cursor: u32,

//     pub fn init(v: u48) Cursor {
//         return .{
//             .segment_idx = @truncate(v >> 32),
//             .segment_cursor = @truncate(v),
//         };
//     }

//     pub fn fromI64(v: i64) Cursor {
//         return Cursor.init(@intCast(v));
//     }
// };
