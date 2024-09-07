const std = @import("std");
const Allocator = std.mem.Allocator;
const AnyWriter = std.io.AnyWriter;
const zigrc = @import("./lib/zigrc.zig");
const Arc = zigrc.Arc;

id: i64,
name: []const u8 = &.{},
created_at: i64,
allocator: Allocator,
rw: std.Thread.RwLock = .{},

const Client = @This();

pub fn init(allocator: Allocator, id: i64) Client {
    return .{ .id = id, .allocator = allocator, .created_at = std.time.milliTimestamp() };
}

pub fn setName(self: *Client, name: []const u8) Allocator.Error!void {
    const allocator = self.allocator;

    self.rw.lock();
    defer self.rw.unlock();

    if (self.name.len > 0) {
        allocator.free(name);
        self.name = &.{};
    }

    self.name = try allocator.dupe(u8, name);
}

pub fn writeName(self: *const Client, w: AnyWriter) anyerror!usize {
    self.rw.lockShared();
    defer self.rw.unlockShared();

    if (self.name.len == 0) {
        return 0;
    }

    return w.write(self.name);
}

pub fn deinit(self: *Client) void {
    const allocator = self.allocator;

    if (self.name.len > 0) {
        allocator.free(self.name);
    }
}

pub const Rc = struct {
    arc: Arc(Client),

    pub fn init(allocator: Allocator, client: Client) Allocator.Error!Rc {
        return .{ .arc = try Arc(Client).init(allocator, client) };
    }

    pub inline fn c(self: *Rc) *Client {
        return self.arc.value;
    }

    pub fn retain(self: *const Rc) Rc {
        return .{ .arc = self.arc.retain() };
    }

    pub fn release(self: Rc) void {
        self.arc.releaseWithFn(Client.deinit, .{});
    }
};

pub const Registry = struct {
    const HashMap = std.AutoHashMap(i64, Client.Rc);

    client_id_counter: i64 = 0,
    clients: HashMap,
    mutex: std.Thread.RwLock,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            // hash map of clients
            .clients = HashMap.init(allocator),
            .mutex = .{},
            .allocator = allocator,
        };
    }

    pub fn registerConnection(self: *Self) Allocator.Error!Client.Rc {
        const id = self.getClientId();
        const allocator = self.allocator;
        const client_rc = try Client.Rc.init(allocator, Client.init(allocator, id));

        self.mutex.lock();
        defer self.mutex.unlock();
        try self.clients.put(id, client_rc);
        return client_rc.retain();
    }

    pub fn dropConnection(self: *Self, id: i64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const client = self.clients.get(id);
        if (client == null) return;
        _ = self.clients.remove(id);
        client.?.releaseWithFn(Client.deinit, .{});
    }

    fn getClientId(self: *Self) i64 {
        return @atomicRmw(i64, &self.client_id_counter, .Add, 1, .monotonic);
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var clientsIter = self.clients.valueIterator();
        while (clientsIter.next()) |c| {
            c.release();
        }
        self.clients.deinit();
    }
};
