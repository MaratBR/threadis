const std = @import("std");
const common = @import("./common.zig");
const buf_util = @import("../buf_util.zig");

const Context = common.Context;

const log = std.log.scoped(.sus);

const scientific_name_of_pig = @embedFile("sus.txt");
const numbers = [_][]const u8{ "One-hundred", "Ninety-nine", "Ninety-eight", "Ninety-seven", "Ninety-six", "Ninety-five", "Ninety-four", "Ninety-three", "Ninety-two", "Ninety-one", "Ninety", "Eighty-nine", "Eighty-eight", "Eighty-seven", "Eighty-six", "Eighty-five", "Eighty-four", "Eighty-three", "Eighty-two", "Eighty-one", "Eighty", "Seventy-nine", "Seventy-eight", "Seventy-seven", "Seventy-six", "Seventy-five", "Seventy-four", "Seventy-three", "Seventy-two", "Seventy-one", "Seventy", "Sixty-nine", "Sixty-eight", "Sixty-seven", "Sixty-six", "Sixty-five", "Sixty-four", "Sixty-three", "Sixty-two", "Sixty-one", "Sixty", "Fifty-nine", "Fifty-eight", "Fifty-seven", "Fifty-six", "Fifty-five", "Fifty-four", "Fifty-three", "Fifty-two", "Fifty-one", "Fifty", "Forty-nine", "Forty-eight", "Forty-seven", "Forty-six", "Forty-five", "Forty-four", "Forty-three", "Forty-two", "Forty-one", "Forty", "Thirty-nine", "Thirty-eight", "Thirty-seven", "Thirty-six", "Thirty-five", "Thirty-four", "Thirty-three", "Thirty-two", "Thirty-one", "Thirty", "Twenty-nine", "Twenty-eight", "Twenty-seven", "Twenty-six", "Twenty-five", "Twenty-four", "Twenty-three", "Twenty-two", "Twenty-one", "Twenty", "Nineteen", "Eighteen", "Seventeen", "Sixteen", "Fifteen", "Fourteen", "Thirteen", "Twelve", "Eleven", "Ten", "Nine", "Eight", "Seven", "Six", "Five", "Four", "Three", "Two", "One" };
var crew_members_text_buf: [300]u8 = undefined;
var crew_members: usize = numbers.len + 1;
var mutex = std.Thread.Mutex{};
const max_number_length = blk: {
    var len: usize = 0;
    for (numbers) |n| {
        if (len < n.len) {
            len = n.len;
        }
    }
    break :blk len;
};

fn handle(ctx: *Context) !void {
    mutex.lock();
    defer mutex.unlock();

    if (ctx.command_arguments >= 1) {
        const maybe_new_value: ?i64 = ctx.readI64() catch null;
        if (maybe_new_value != null) {
            const new_value: i64 = maybe_new_value.?;
            if (new_value >= 0) {
                crew_members = @intCast(new_value);
            }
        }
        try ctx.discardRemainingArguments();
    } else if (crew_members > 0) {
        crew_members = crew_members - 1;
    }

    if (crew_members == 0) {
        try ctx.redis_writer.writeComptimeSimpleString(scientific_name_of_pig);
    } else {
        if (crew_members > numbers.len) {
            crew_members = numbers.len;
        }

        var bb = buf_util.StaticBinaryBuilder.init(&crew_members_text_buf);
        writeVerse(&bb) catch |err| {
            log.err("error occured while trying to write the verse: {}", .{err});
            try ctx.redis_writer.writeError("oops, something went wrong");
            return;
        };

        const str = bb.buf[0..bb.cursor];
        std.debug.print("str: {s}\n", .{str});

        try ctx.redis_writer.writeSimpleString(str);
    }
}

fn writeVerse(bb: *buf_util.StaticBinaryBuilder) !void {
    std.debug.assert(crew_members <= numbers.len);
    try bb.push(numbers[numbers.len - crew_members]);
    if (crew_members == 1) {
        try bb.push(" crew member on the wall,\n");
    } else {
        try bb.push(" crew members on the wall,\n");
    }
    try bb.push(numbers[numbers.len - crew_members]);
    if (crew_members == 1) {
        try bb.push(" crew member!\n");
    } else {
        try bb.push(" crew members!\n");
    }

    if (crew_members <= 1) {
        try bb.push("Take it down,\nPass it around,\nNo more crew members on the wall!");
    } else {
        try bb.push("Take one down,\nPass it around,\n");
        try bb.push(numbers[numbers.len - crew_members + 1]);
        try bb.push(" crew members on the wall!");
    }
}

pub const sus = common.CommandHandler(.{ .name = "sus", .decl = .{ .handler = handle } });
