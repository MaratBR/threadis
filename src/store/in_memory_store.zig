const std = @import("std");

const UnmanagedEntryValue = @import("./value.zig");

const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;

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

const Segment = struct {
    // max size of this segment in bytes
    max_size: usize,

    // lookup table containing all values store in the segment
    lookup_table: StringHashMap(*Entry),

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
            .lookup_table = StringHashMap(*Entry).init(allocator),

            // atomic counter
            .count = 0,
        };
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
        self.mutex.lock();
        self.lookup_table.put(key, entry_ptr) catch unreachable; // OutOfMemory
        self.mutex.unlock();
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
    segments_count: usize,
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
            segments[i] = Segment.init(allocator);
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

    pub fn deinit(self: *Self) void {
        const allocator = self.allocator;
        for (self.segments) |*segment| {
            segment.deinit();
        }
        allocator.free(self.segments);
    }
};
