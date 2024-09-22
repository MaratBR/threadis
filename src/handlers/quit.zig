const common = @import("./common.zig");

const Context = common.Context;

fn handle(_: *Context) !void {
    return error.Quit;
}

pub const quit = common.CommandHandler(.{ .name = "quit", .handler = handle });
