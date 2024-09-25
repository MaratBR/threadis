const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;
const Store = @import("../store/store.zig").Store;

fn handleIncr(ctx: *Context) !void {
    if (!try ctx.minArgNum(1))
        return;

    const maybe_key = try ctx.readString();
    if (maybe_key == null) {
        try ctx.redis_writer.writeError("invalid 1st argument for 'incr' command");
        try ctx.discardRemainingArguments();
        return;
    }
    const key = maybe_key.?;
    defer key.deinit();

    try add(ctx, key.buf, 1);
}

fn handleDecr(ctx: *Context) !void {
    if (!try ctx.minArgNum(1))
        return;

    const maybe_key = try ctx.readString();
    if (maybe_key == null) {
        try ctx.redis_writer.writeError("invalid 1st argument for 'decr' command");
        try ctx.discardRemainingArguments();
        return;
    }
    const key = maybe_key.?;
    defer key.deinit();

    try add(ctx, key.buf, -1);
}

fn handleIncrBy(ctx: *Context) !void {
    if (!try ctx.minArgNum(2))
        return;

    const maybe_key = try ctx.readString();
    if (maybe_key == null) {
        try ctx.redis_writer.writeError("invalid 1st argument for 'decr' command");
        try ctx.discardRemainingArguments();
        return;
    }
    const key = maybe_key.?;
    defer key.deinit();

    const by = try ctx.readI64();
    try add(ctx, key.buf, by);
}

fn handleDecrBy(ctx: *Context) !void {
    if (!try ctx.minArgNum(2))
        return;

    const maybe_key = try ctx.readString();
    if (maybe_key == null) {
        try ctx.redis_writer.writeError("invalid 1st argument for 'decr' command");
        try ctx.discardRemainingArguments();
        return;
    }
    const key = maybe_key.?;
    defer key.deinit();

    const by = try ctx.readI64();
    try add(ctx, key.buf, -by);
}

fn add(ctx: *Context, key: []const u8, value: i64) !void {
    const maybe_entry = ctx.store.get(key);

    var new_value: i64 = undefined;

    if (maybe_entry == null) {
        const entry_value = Store.Value.initI64(value);
        try ctx.store.put(key, &entry_value);
        new_value = value;
    } else {
        var borrowed_entry = maybe_entry.?;
        defer borrowed_entry.release();

        if (borrowed_entry.entry.value.type != .i64) {
            try ctx.redis_writer.writeError("cannot perform incr or decr operation on non-integer value");
            return;
        }

        new_value = borrowed_entry.entry.value.raw.i64 + value;
        const add_result = @addWithOverflow(borrowed_entry.entry.value.raw.i64, value);
        if (add_result[1] == 1) {
            try ctx.redis_writer.writeError("operation resulted in integer overflow");
            return;
        }

        const entry_value = Store.Value.initI64(add_result[0]);
        try borrowed_entry.entry.set(&entry_value);
    }

    try ctx.redis_writer.writeI64(new_value);
}

pub const incr = common.CommandHandler(.{ .name = "incr", .decl = .{ .arity = 2, .handler = handleIncr } });
pub const decr = common.CommandHandler(.{ .name = "decr", .decl = .{ .arity = 2, .handler = handleDecr } });
pub const incrby = common.CommandHandler(.{ .name = "incrby", .decl = .{ .arity = 3, .handler = handleIncrBy } });
pub const decrby = common.CommandHandler(.{ .name = "decrby", .decl = .{ .arity = 3, .handler = handleDecrBy } });
