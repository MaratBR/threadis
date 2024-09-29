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
    try ctx.redis_writer.writeArrayHeader(0);

    // inline for (command_handlers) |h| {
    //     inline for (h.inner) |_decl| {
    //         const decl: common.CommandDecl = _decl;
    //         try ctx.redis_writer.writeArrayHeader(6);
    //         try ctx.redis_writer.writeComptimeSimpleString(decl.name);
    //         try ctx.redis_writer.writeI64(decl.arity);
    //         try ctx.redis_writer.writeEmptyArray();
    //         try ctx.redis_writer.writeI64(decl.pos_first_key);
    //         try ctx.redis_writer.writeI64(decl.pos_last_key);
    //         try ctx.redis_writer.writeI64(decl.step_count_keys);
    //     }
    // }
}

const debug_commands = @import("debug.zig");

pub const command_handler = common.Commands(.{
    // append handler
    .append = @import("append.zig").append,

    // client handler
    .client = @import("client.zig").client,

    // reading handlers
    .get = @import("get.zig").get,
    .scan = @import("scan.zig").scan,

    // set handler
    .set = @import("set.zig").set,

    // ping handler
    .ping = @import("ping.zig").ping,

    // quit handler
    .quit = @import("quit.zig").quit,

    // incr handler
    .incr = @import("incr_decr.zig").incr,
    // decr handler
    .decr = @import("incr_decr.zig").decr,
    // quiincrbyt handler
    .incrby = @import("incr_decr.zig").incrby,
    // decrby handler
    .decrby = @import("incr_decr.zig").decrby,

    // silly commands
    .deeznuts = @import("deez_nuts.zig").deeznuts,
    .sus = @import("sus.zig").sus,

    // debug commands
    .debug_populate = debug_commands.debug_populate,

    // command handler
    .command = common.CommandHandler(.{ .name = "command", .decl = .{ .handler = sendListOfCommands } }),
});

pub fn getCommandHandler(_: []const u8) CommandHandlerFn {
    // pretty inefficient, fix it in the future
    // inline for (command_handlers) |cmd_decl| {
    //     inline for (cmd_decl.inner) |_cmd_decl2| {
    //         const decl: common.CommandDecl = _cmd_decl2;
    //         if (decl.is(command)) {
    //             return decl.handler;
    //         }
    //     }
    // }

    return command_handler.handle;
}
