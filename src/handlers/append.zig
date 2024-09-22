const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;
const Store = @import("../store/store.zig").Store;

fn handle(ctx: *Context) !void {
    if (!try ctx.minArgNum(2))
        return;

    const maybe_key = try ctx.readString();
    if (maybe_key == null) {
        try ctx.redis_writer.writeError("invalid 1st argument for 'append' command");
        try ctx.discardRemainingArguments();
        return;
    }
    const key = maybe_key.?;
    defer key.deinit();
    const maybe_value = try ctx.readString();

    if (maybe_value == null) {
        return;
    }

    const value = maybe_value.?;
    defer value.deinit();

    const maybe_entry = ctx.store.get(key.buf);

    if (maybe_entry == null) {
        const entry_value = Store.Value.initBinary(value.buf);

        try ctx.store.put(key.buf, &entry_value);
        try ctx.redis_writer.writeUsize(value.buf.len);
    } else {
        var borrowed_entry = maybe_entry.?;
        defer borrowed_entry.release();

        try borrowed_entry.entry.append(value.buf);
        try ctx.redis_writer.writeUsize(borrowed_entry.entry.len());
    }
}

pub const append = common.CommandHandler(.{ .name = "append", .flags = .{ .write = true }, .arity = 2, .handler = handle });
