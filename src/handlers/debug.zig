const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;
const Store = @import("../store/store.zig").Store;

fn handlePopulate(ctx: *Context) !void {
    if (!try ctx.exactArgNum(1)) return;
    const count = try ctx.readI64();

    if (count > 0) {
        const countus: usize = @intCast(count);
        for (0..countus) |i| {
            var buf: [40]u8 = undefined;
            const key = try std.fmt.bufPrint(&buf, "{}", .{i});
            const value = Store.Value.initBinary(key);
            try ctx.store.put(key, &value);
        }
    }

    try ctx.discardRemainingArguments();
}

pub const debug_populate = common.CommandHandler(.{ .name = "debug_populate", .decl = .{ .arity = 1, .handler = handlePopulate } });
