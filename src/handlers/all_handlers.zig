const std = @import("std");
const common = @import("common.zig");
const CommandHandlerFn = common.CommandHandlerFn;
const redis = @import("../redis.zig");

pub const Context = common.Context;

fn commandNoop(ctx: *Context) !void {
    std.log.warn("skipping unimplemented command '{s}'", .{ctx.command});
    try ctx.discardRemainingArguments();
}

fn sendListOfCommands(ctx: *Context) !void {
    try ctx.discardRemainingArguments();

    try ctx.redis_writer.writeArrayHeader(@intCast(command_handlers.len));

    inline for (command_handlers) |h| {
        const decl: common.CommandDecl = h.decl;

        try ctx.redis_writer.writeArrayHeader(6);
        try ctx.redis_writer.writeComptimeSimpleString(decl.name);
        try ctx.redis_writer.writeI64(decl.arity);
        try ctx.redis_writer.writeEmptyArray();
        try ctx.redis_writer.writeI64(decl.pos_first_key);
        try ctx.redis_writer.writeI64(decl.pos_last_key);
        try ctx.redis_writer.writeI64(decl.step_count_keys);
    }
}

pub const command_command = common.CommandHandler(.{ .name = "command", .handler = sendListOfCommands });

pub const command_handlers = .{
    // append command
    @import("append.zig").append,

    // client command
    @import("client.zig").client,

    // get command
    @import("get.zig").get,

    // set command
    @import("set.zig").set,

    // ping command
    @import("ping.zig").ping,

    // quit command
    @import("quit.zig").quit,

    // command command
    command_command,
};

pub fn getCommandHandler(command: []const u8) CommandHandlerFn {
    // pretty inefficient, fix it in the future
    inline for (command_handlers) |h| {
        if (h.decl.is(command)) {
            return h.decl.handler;
        }
    }

    return commandNoop;
}
