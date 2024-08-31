const std = @import("std");
const Allocator = std.mem.Allocator;
const Address = std.net.Address;

pub const Client = struct {
    id: i64,

    name: []const u8 = &.{},

    connected_at: i64,

    address: Address,

    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, id: i64, address: Address) Self {
        return .{ .id = id, .allocator = allocator, .address = address, .connected_at = std.time.milliTimestamp() };
    }

    pub fn deinit(self: *Self) void {
        const allocator = self.allocator;

        if (self.name.len > 0) {
            allocator.free(self.name);
        }
    }
};

pub const ClientRegistry = struct {
    client_id_counter: i64 = 0,
    clients: std.AutoHashMap(i64, Client),
    mutex: std.Thread.RwLock,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return .{
            // hash map of clients
            .clients = std.AutoHashMap(i64, Client).init(allocator),
            .mutex = .{},
            .allocator = allocator,
        };
    }

    pub fn registerConnection(self: *Self, address: Address) Allocator.Error!*Client {
        const id = self.getClientId();
        self.mutex.lock();
        defer self.mutex.unlock();
        const allocator = self.allocator;
        const client = Client.init(allocator, id, address);
        try self.clients.put(id, client);
        return self.clients.getPtr(id).?;
    }

    pub fn dropConnection(self: *Self, id: i64) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        _ = self.clients.remove(id);
    }

    fn getClientId(self: *Self) i64 {
        return @atomicRmw(i64, &self.client_id_counter, .Add, 1, .monotonic);
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var clientsIter = self.clients.valueIterator();
        while (clientsIter.next()) |c| {
            c.deinit();
        }
        self.clients.deinit();
    }
};
