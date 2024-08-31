const std = @import("std");
const posix = std.posix;
const coro = @import("coro");
const aio = @import("aio");

const Allocator = std.mem.Allocator;

pub const ServerOptions = struct { addr: std.net.Address };

pub const AcceptError = coro.io.Error || aio.Accept.Error;

pub const StartError = coro.io.Error || aio.Socket.Error || std.posix.SetSockOptError || std.posix.BindError || std.posix.ListenError;

pub const Server = struct {
    pub const Connection = struct {
        client_sock: posix.socket_t,

        client_addr: std.net.Address,

        pub fn reader(self: *Connection, allocator: Allocator) Reader {
            return Reader.init(allocator, get_page_size(), self.client_sock);
        }

        pub fn close(self: *Connection) !void {
            try coro.io.single(aio.CloseSocket{ .socket = self.client_sock });
        }
    };

    options: ServerOptions,
    sock: ?posix.socket_t = null,
    mutex: std.Thread.Mutex = .{},

    const Self = @This();

    pub fn init(options: ServerOptions) Self {
        return .{
            .options = options,
        };
    }

    pub fn start(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var socket: std.posix.socket_t = undefined;
        try coro.io.single(aio.Socket{
            .domain = std.posix.AF.INET,
            .flags = std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC,
            .protocol = std.posix.IPPROTO.TCP,
            .out_socket = &socket,
        });

        try std.posix.setsockopt(socket, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        if (@hasDecl(std.posix.SO, "REUSEPORT")) {
            try std.posix.setsockopt(socket, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
        }

        const address = self.options.addr;
        try std.posix.bind(socket, &address.any, address.getOsSockLen());
        try std.posix.listen(socket, 128);
    }

    pub fn accept(self: *Self) AcceptError!Connection {
        std.debug.assert(self.sock != null);
        const sock = self.sock.?;

        self.mutex.lock();
        defer self.mutex.unlock();

        var client_sock: posix.socket_t = undefined;
        var client_addr: posix.sockaddr = undefined;
        var err: aio.Accept.Error = undefined;

        try coro.io.single(aio.Accept{ .socket = sock, .out_socket = &client_sock, .out_addr = &client_addr, .out_error = &err });

        if (err != error.Success) {
            return err;
        }

        // const addr = std.net.Address{ .any = client_addr };

        return .{ .client_addr = undefined, .client_sock = client_sock };
    }
};

fn get_page_size() usize {
    return 4096;
}

pub const Reader = struct {
    sock: posix.socket_t,
    buf: []u8,
    len: usize = 0,
    pos: usize = 0,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, buf_size: usize, sock: posix.socket_t) Allocator.Error!Self {
        std.debug.assert(buf_size > 0);
        const buf = try allocator.alloc(u8, buf_size);

        return .{ .sock = sock, .buf = buf, .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        const allocator = self.allocator;
        allocator.free(self.buf);
    }

    pub fn any(self: *Self) std.io.AnyReader {
        return .{ .context = self, .readFn = Self.read };
    }

    pub fn read(self: *Self, dest: []const u8) !usize {
        std.debug.assert(dest.len > 0);
        const bytes_in_buf = self.len - self.pos;

        // buffer contains more or equal amount of data than was requested
        if (dest.len <= bytes_in_buf) {
            std.debug.assert(self.buf.len >= self.pos + dest.len);
            std.mem.copyForwards(u8, dest, self.buf[self.pos .. self.pos + dest.len]);
            self.pos += dest.len;
            return dest.len;
        }

        // copy data remaining in the buffer to dest
        if (bytes_in_buf > 0) {
            std.mem.copyForwards(u8, dest, self.buf[self.pos..]);
        }

        var dest_offset = bytes_in_buf;
        std.debug.assert(dest.len - dest_offset > 0);

        while (dest.len - dest_offset > 0) {
            // load next buf
            try coro.io.single(aio.Recv{ .socket = self.sock, .buffer = self.buf, .out_read = &self.len });

            const remaining_bytes = dest.len - dest_offset;

            if (remaining_bytes <= self.len) {
                // read enough
                std.mem.copyForwards(u8, dest[dest_offset..], self.buf[0..remaining_bytes]);
                self.pos = remaining_bytes;
                dest_offset += remaining_bytes;
                std.debug.assert(dest_offset == dest.len);
            } else if (self.len < self.buf.len) {
                // last buffer contains less data than was requested meaning we have reached
                // the end of the client message so we stop here even though not all data was read

                if (self.len != 0) {
                    std.mem.copyForwards(u8, dest[dest_offset .. dest_offset + self.len], self.buf[0..self.len]);
                    dest_offset += self.len;
                    self.pos = self.len;
                    std.debug.assert(dest_offset < dest.len);
                } else {
                    // connection was dropped most likely
                    self.pos = 0;
                }
                break;
            } else {
                std.debug.assert(self.len == self.buf.len);
                std.mem.copyForwards(u8, dest[dest_offset .. dest_offset + self.len], self.buf);
                dest_offset += self.len;
                // no need to update pos
            }
        }

        std.debug.assert(self.pos <= self.len);
        const read_bytes = dest_offset;
        return read_bytes;
    }
};
