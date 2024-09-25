const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;
const Store = @import("../store/store.zig").Store;

fn handle(ctx: *Context) !void {
    if (!try ctx.minArgNum(2))
        return;

    const maybe_key = try ctx.readString();
    if (maybe_key == null) {
        try ctx.redis_writer.writeError("invalid 1st argument for 'set' command");
        try ctx.discardRemainingArguments();
        return;
    }
    const key = maybe_key.?;
    defer key.deinit();
    const maybe_value = try ctx.readString();

    const maybe_entry = ctx.store.get(key.buf);

    if (maybe_entry == null) {
        if (maybe_value == null) {
            // do nothing
        } else {
            const value = maybe_value.?;
            defer value.deinit();
            const entry_value = Store.Value.initBinary(value.buf);
            try ctx.store.put(key.buf, &entry_value);
        }
    } else {
        var borrowed_entry = maybe_entry.?;
        defer borrowed_entry.release();

        if (maybe_value == null) {
            ctx.store.del(key.buf);
        } else {
            const value = maybe_value.?;
            defer value.deinit();
            const entry_value = Store.Value.initBinary(value.buf);
            try borrowed_entry.entry.set(&entry_value);
        }
    }

    try ctx.redis_writer.writeOK();
}

pub const set = common.CommandHandler(.{ .name = "set", .decl = .{ .arity = 3, .handler = handle } });
