const std = @import("std");
const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;

pub const EntryUntypedValue = union { i64: i64, f64: f64, bool: bool, binary: []u8 };

pub const EntryValueType = enum { i64, f64, bool, binary };

pub const EntryValue = struct {
    type: EntryValueType,
    raw: EntryUntypedValue,

    const Self = @This();

    pub fn init_i64(v: i64) Self {
        return .{ .type = EntryValueType.i64, .raw = .{ .i64 = v } };
    }

    pub fn init_f64(v: f64) Self {
        return .{ .type = EntryValueType.f64, .raw = .{ .f64 = v } };
    }

    pub fn init_bool(v: bool) Self {
        return .{ .type = EntryValueType.bool, .raw = .{ .bool = v } };
    }

    pub fn init_binary(v: []const u8) Self {
        return .{ .type = EntryValueType.binary, .raw = .{ .binary = v } };
    }

    pub fn deinit_with_allocator(self: *Self, allocator: Allocator) void {
        switch (self.type) {
            EntryValueType.binary => {
                allocator.free(self.raw.binary);
            },
            EntryValueType.bool, EntryValueType.f64, EntryValueType.i64 => {},
        }
    }
};

pub const UnmanagedEntry = struct {
    value: EntryValue,
    added_at: i64,
    _rc: u16,

    const Self = @This();

    pub fn init(value: EntryValue) Self {
        return .{
            .added_at = 0,

            // in-memory store itself has ref to this entry
            ._rc = 1,

            .value = value,
        };
    }

    pub fn isBorrowed(self: *Self) bool {
        return @atomicLoad(u16, &self._rc, .monotonic) > 0;
    }

    pub fn borrow(self: *Self, allocator: Allocator) Borrowed {
        std.debug.assert(@atomicLoad(u16, &self._rc, .monotonic) < std.math.maxInt(u16));
        _ = @atomicRmw(u16, &self._rc, .Add, 1, .monotonic);
        return .{ ._ptr = self, ._allocator = allocator, ._released = false };
    }

    fn release_with_allocator(self: *Self, allocator: Allocator) void {
        if (@atomicRmw(u16, &self._rc, .Sub, 1, .monotonic) != 1) {
            return;
        }

        self.deinit_with_allocator(allocator);
        return;
    }

    pub fn deinit_with_allocator(self: *Self, allocator: Allocator) void {
        std.debug.assert(!self.isBorrowed());

        self.value.deinit_with_allocator(allocator);
    }

    pub const Borrowed = struct {
        _unmanaged_ptr: *UnmanagedEntry,
        _allocator: Allocator,
        _released: bool,

        pub fn value(self: *const Borrowed) *EntryValue {
            std.debug.assert(!self._released);

            return &self._unmanaged_ptr.value;
        }

        pub fn release(self: *const Borrowed) void {
            std.debug.assert(!self._released);

            @atomicStore(bool, &self._released, true, .monotonic);
            const allocator = self._allocator;
            self._unmanaged_ptr.release_with_allocator(allocator);
        }

        pub fn deinit(self: *const Borrowed) void {
            self.release();
        }
    };
};

const Segment = struct {
    // max size of this segment in bytes
    max_size: usize,

    // lookup table containing all values store in the segment
    lookup_table: StringHashMap(UnmanagedEntry),

    // mutex for accesing the values stored
    mutex: std.Thread.RwLock,

    count: usize,

    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{

            // max size of this segment in bytes (WIP)
            .max_size = 100,

            .allocator = allocator,

            // mutex for reads and writes to the segment
            .mutex = .{},

            // in memory hash map
            .lookup_table = StringHashMap(UnmanagedEntry).init(allocator),

            // atomic counter
            .count = 0,
        };
    }

    pub fn get(self: *Self, key: []const u8) ?UnmanagedEntry.Borrowed {
        std.debug.assert(key.len > 0);

        return self.getInternal(key);
    }

    pub fn put(self: *Self, key: []const u8, value: UnmanagedEntry) void {
        std.debug.assert(!value.isBorrowed());
        std.debug.assert(key.len > 0);

        self.mutex.lock();
        self.lookup_table.put(key, value) catch unreachable; // OutOfMemory
        self.mutex.unlock();
    }

    pub fn putValue(self: *Self, key: []const u8, value: EntryValue) void {
        std.debug.assert(key.len > 0);

        const entry = UnmanagedEntry.init(value);
        self.mutex.lock();
        self.lookup_table.put(key, entry) catch unreachable; // OutOfMemory
        self.mutex.unlock();
    }

    pub fn del(self: *Self, key: []const u8) bool {
        std.debug.assert(key.len > 0);

        self.mutex.lock();
        const ptr = self.lookup_table.getPtr(key) orelse return false;
        _ = self.lookup_table.removeByPtr(ptr);
        _ = ptr.release();
        self.mutex.unlock();
        return true;
    }

    fn getInternal(self: *Self, key: []const u8) ?UnmanagedEntry.Borrowed {
        self.mutex.lockShared();
        const entry_opt = self.lookup_table.getPtr(key);
        const entry: *UnmanagedEntry = entry_opt orelse {
            self.mutex.unlockShared();
            return null;
        };

        const allocator = self.allocator;
        const entry_borrowed = entry.borrow(allocator);
        self.mutex.unlockShared();
        return entry_borrowed;
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();

        const allocator = self.allocator;

        for (self.lookup_table.valueIterator()) |*value| {
            value.deinit_with_allocator(allocator);
        }
        self.mutex.unlock();

        self.lookup_table.deinit();
    }
};

pub const InMemoryStoreConfig = struct {
    segments_count: usize,
};

pub const InMemoryStore = struct {
    segments: []Segment,
    allocator: Allocator,
    segment_mask: usize,

    const Self = @This();

    pub fn init(allocator: Allocator, config: InMemoryStoreConfig) !Self {
        const segment_count = config.segments_count;
        if (segment_count == 0) return error.SegmentBucketNotPower2;
        // has to be a power of 2
        if ((segment_count & (segment_count - 1)) != 0) return error.SegmentBucketNotPower2;

        const segments = try allocator.alloc(Segment, segment_count);
        for (0..segment_count) |i| {
            segments[i] = Segment.init(allocator);
        }

        return .{ .segments = segments, .allocator = allocator, .segment_mask = segment_count - 1 };
    }

    pub fn get(self: *Self, key: []const u8) ?UnmanagedEntry.Borrowed {
        return self.getSegment(key).get(key);
    }

    pub fn put(self: *Self, key: []const u8, entry: UnmanagedEntry) void {
        self.getSegment(key).put(key, entry);
    }

    pub fn putValue(self: *Self, key: []const u8, value: EntryValue) void {
        self.getSegment(key).putValue(key, value);
    }

    pub fn del(self: *Self, key: []const u8) void {
        self.getSegment(key).put(key);
    }

    fn getSegment(self: *Self, key: []const u8) *Segment {
        const hash: u64 = std.hash.Wyhash.hash(0, key);
        return &self.segments[self.segment_mask & hash];
    }

    pub fn deinit(self: *Self) void {
        const allocator = self.allocator;
        for (self.segments) |*segment| {
            segment.deinit();
        }
        allocator.free(self.segments);
    }
};
