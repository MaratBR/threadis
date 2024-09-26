const common = @import("./common.zig");

const Context = common.Context;

fn handle(ctx: *Context) !void {
    try ctx.discardRemainingArguments();
    try ctx.redis_writer.writeComptimeSimpleString("Haha, very funny");
}

pub const deeznuts = common.CommandHandler(.{ .name = "deeznuts", .decl = .{ .handler = handle } });
