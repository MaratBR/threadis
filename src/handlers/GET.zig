const common = @import("./common.zig");

const Context = common.Context;

pub fn get(ctx: *Context) !void {
    if (ctx.command_arguments != 1) {
        return error.InvalidNumberOfArguments;
    }

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
