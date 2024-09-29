const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;

fn handle(ctx: *Context) !void {
    const args = try ctx.readParameters(struct { cursor: ?i64 }, struct { match: ?[]const u8 });
    const cursor = args.positional_args.cursor orelse 0;

    std.debug.print("cursor={} match={?s}\n", .{ cursor, args.flags.match });

    var it = ctx.store.scan(cursor, 3, "*");
    defer it.deinit();

    var keys = std.ArrayList([]const u8).init(ctx.allocator);
    defer keys.deinit();

    while (it.nextSegment()) |seg_iterator| {
        while (seg_iterator.next()) |key| {
            try keys.append(key);
        }
    }

    const next_cursor = it.cursor();

    try ctx.redis_writer.writeArrayHeader(2);
    try ctx.redis_writer.writeI64(next_cursor);
    try ctx.redis_writer.writeArrayHeader(keys.items.len);

    for (keys.items) |key| {
        try ctx.redis_writer.writeBulkString(key);
    }

    try ctx.discardRemainingArguments();
}

pub const scan = common.CommandHandler(.{ .name = "scan", .decl = .{ .arity = 1, .handler = handle } });
