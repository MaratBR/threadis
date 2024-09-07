const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;

pub fn CLIENT(ctx: *Context) !void {
    const log = std.log.scoped(.CLIENT);

    if (!try ctx.minArgNum(1)) return;

    const maybe_sub_command = ctx.readEnum(common.redis.ClientCommand) catch |e| {
        if (e == error.InvalidEnum) {
            log.info("invalid subcommand received", .{});
            return error.InvalidSubcommand;
        } else {
            return e;
        }
        return;
    };

    if (maybe_sub_command == null) {
        try ctx.discardRemainingArguments();
        try ctx.redis_writer.writeError("invalid sub-command for command 'client'");
        return;
    }

    const sub_command = maybe_sub_command.?;

    switch (sub_command) {
        .ID => {
            if (!try ctx.exactArgNum(1)) return;
            try ctx.redis_writer.writeI64(ctx.client.c().id);
            log.info("CLIENT ID to {}", .{ctx.conn_address});
        },

        else => {
            try ctx.discardRemainingArguments();
            return;
        },
    }
}
