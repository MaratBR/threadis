const common = @import("./common.zig");

const Context = common.Context;

pub fn quit(_: *Context) !void {
    return error.Quit;
}
