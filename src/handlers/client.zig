const std = @import("std");
const common = @import("./common.zig");
const ctmap = @import("ctmap.zig");

const Context = common.Context;

fn handle(ctx: *Context) !void {
    if (!try ctx.minArgNum(1)) return;

    const sub_command_handler: ?common.CommandHandlerFn = try common.readSubcommandHandler(ctx, .{ .id = &id, .setname = &setname });

    if (sub_command_handler == null) {
        try ctx.discardRemainingArguments();
        try ctx.redis_writer.writeError("invalid sub-command for command 'client'");
        return;
    }

    try sub_command_handler.?(ctx);
}

fn id(ctx: *Context) anyerror!void {
    if (!try ctx.exactArgNum(1)) return;
    try ctx.redis_writer.writeI64(ctx.client.c().id);
}

fn setname(ctx: *Context) anyerror!void {
    if (!try ctx.exactArgNum(1)) return;
    const maybe_name = try ctx.readString();
    if (maybe_name) |name| {
        defer name.deinit();
        try ctx.client.c().setName(name.buf);
    } else {
        ctx.client.c().removeName();
    }
}

fn getname(ctx: *Context) anyerror!void {
    if (!try ctx.exactArgNum(2)) return;
    const maybe_name = try ctx.readString();
    if (maybe_name) |name| {
        defer name.deinit();
        try ctx.client.c().setName(name.buf);
    } else {
        ctx.client.c().removeName();
    }
}

pub const client = common.CommandHandler(.{ .name = "client", .decl = .{ .arity = -2, .handler = handle } });
