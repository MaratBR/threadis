const std = @import("std");
const common = @import("./common.zig");

const Context = common.Context;

fn handle(ctx: *Context) !void {
    const log = std.log.scoped(.client);

    if (!try ctx.minArgNum(1)) return;

    const maybe_sub_command = ctx.readEnum(enum { id, setname, getname, kill, list, pause, reply, unblock }) catch |e| {
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
        .id => {
            if (!try ctx.exactArgNum(1)) return;
            try ctx.redis_writer.writeI64(ctx.client.c().id);
            log.info("CLIENT ID to {}", .{ctx.conn_address});
        },

        .setname => {
            if (!try ctx.exactArgNum(2)) return;
            const maybe_name = try ctx.readString();
            if (maybe_name) |name| {
                defer name.deinit();
                try ctx.client.c().setName(name.buf);
            } else {
                ctx.client.c().removeName();
            }
        },

        else => {
            try ctx.discardRemainingArguments();
            return;
        },
    }
}

pub const client = common.CommandHandler(.{ .name = "client", .decl = .{ .arity = -2, .handler = handle } });
