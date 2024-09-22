const common = @import("./common.zig");

const Context = common.Context;

fn handle(ctx: *Context) !void {
    if (!try ctx.exactArgNum(0)) return;
    try ctx.redis_writer.writeComptimeSimpleString("PONG");
}

pub const ping = common.CommandHandler(.{ .name = "ping", .handler = handle });
