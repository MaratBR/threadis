const common = @import("./common.zig");

const Context = common.Context;

pub fn ping(ctx: *Context) !void {
    if (!try ctx.exactArgNum(0)) return;
    try ctx.redis_writer.writeComptimeSimpleString("PONG");
}
