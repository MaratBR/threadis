const std = @import("std");
const posix = std.posix;
const coro = @import("coro");
const aio = @import("aio");
const Allocator = std.mem.Allocator;

pub const Options = struct { addr: std.net.Address };

pub const Connection = struct {
    client_sock: posix.socket_t,

    client_addr: std.net.Address,

    pub fn reader(self: Connection, allocator: Allocator) Allocator.Error!Reader {
        return Reader.init(allocator, 4096, self.client_sock);
    }

    pub fn writer(self: Connection) Allocator.Error!Writer {
        return Writer.init(self.client_sock);
    }

    pub fn close(self: Connection) !void {
        try coro.io.single(aio.CloseSocket{ .socket = self.client_sock });
    }

    pub const Writer = struct {
        sock: posix.socket_t,

        const Self = @This();

        pub fn init(sockfd: posix.socket_t) Self {
            return .{ .sock = sockfd };
        }

        pub fn write(self: Self, buf: []const u8) !usize {
            var written: usize = undefined;
            try coro.io.single(aio.Send{ .buffer = buf, .out_written = &written, .socket = self.sock });
            return written;
        }

        pub fn any(self: *Self) std.io.AnyWriter {
            return .{ .context = @ptrCast(self), .writeFn = typeErasedWriteFn };
        }

        fn typeErasedWriteFn(context: *const anyopaque, buffer: []const u8) anyerror!usize {
            const ptr: *const Writer = @alignCast(@ptrCast(context));
            return Writer.write(ptr.*, buffer);
        }

        pub fn deinit(_: Self) void {}
    };

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
            return .{ .context = @ptrCast(self), .readFn = typeErasedReadFn };
        }

        fn typeErasedReadFn(context: *const anyopaque, buffer: []u8) anyerror!usize {
            const ptr: *Reader = @constCast(@alignCast(@ptrCast(context)));
            return Reader.read(ptr, buffer);
        }

        pub fn read(self: *Self, dest: []u8) !usize {
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
};

pub const StartError = coro.io.Error || aio.Socket.Error || std.posix.SetSockOptError || std.posix.BindError || std.posix.ListenError;

pub fn create(options: Options) StartError!posix.socket_t {
    const socket = try posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC, std.posix.IPPROTO.TCP);
    // errdefer std.posix.close(socket);
    try std.posix.setsockopt(socket, std.posix.SOL.SOCKET, std.posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
    if (@hasDecl(std.posix.SO, "REUSEPORT")) {
        try std.posix.setsockopt(socket, std.posix.SOL.SOCKET, std.posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
    }

    const address = options.addr;
    try std.posix.bind(socket, &address.any, address.getOsSockLen());
    try std.posix.listen(socket, 128);

    return socket;
}

pub const AcceptError = posix.AcceptError;

pub fn acceptConnection(sockfd: posix.socket_t) AcceptError!Connection {
    var client_addr: posix.sockaddr = undefined;
    var client_addr_len: posix.socklen_t = @sizeOf(@TypeOf(client_addr));

    const client_sock = try posix.accept(sockfd, &client_addr, &client_addr_len, 0);
    const addr = std.net.Address{ .any = client_addr };
    return .{ .client_addr = addr, .client_sock = client_sock };
}

pub fn close(sockfd: posix.socket_t) void {
    switch (@import("builtin").os.tag) {
        .windows => std.os.windows.closesocket(sockfd) catch |win_err| {
            std.log.err("failed to close socket on windows: {}", .{win_err});
        },
        else => posix.close(sockfd),
    }
}
