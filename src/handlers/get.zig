const common = @import("./common.zig");

const Context = common.Context;

fn handle(ctx: *Context) !void {
    if (!try ctx.exactArgNum(1)) return;

    const maybe_str = try ctx.readString();

    if (maybe_str == null) {
        try ctx.redis_writer.writeNull();
    } else {
        const key = maybe_str.?;
        defer key.deinit();

        const maybe_entry = ctx.store.get(key.buf);

        if (maybe_entry == null) {
            try ctx.redis_writer.writeNull();
        } else {
            var borrowed_entry = maybe_entry.?;
            defer borrowed_entry.release();
            try ctx.redis_writer.writeValue(&borrowed_entry.entry.value);
        }
    }
}

pub const get = common.CommandHandler(.{ .name = "get", .arity = 2, .handler = handle });
