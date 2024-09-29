const std = @import("std");

pub fn glob(pattern: []const u8, test_string: []const u8) bool {
    return globDepth(pattern, test_string, 8);
}

pub fn globDepth(pattern: []const u8, test_string: []const u8, comptime max_depth: u8) bool {
    if (max_depth == 0) {
        @compileError("max_depth cannot be 0");
    }

    if (pattern.len == 0 or test_string.len == 0) return false;

    if (pattern.len == 1 and pattern[0] == '*') {
        return true;
    }

    return globRecursively(pattern, test_string, max_depth) == .match;
}

const GlobResult = enum(u8) { match, no_match, recursion };

fn globRecursively(pattern: []const u8, test_string: []const u8, depth: u8) GlobResult {
    std.debug.assert(pattern.len > 0);

    var pr = StringReader.init(pattern);
    var sr = StringReader.init(test_string);
    var escaped = false;

    outer: while (true) {
        var pc = pr.next();

        if (pc == 0) {
            // reached end of the pattern
            // if also reached end of the payload then it's a match, otherwise - no
            if (sr.pos == sr.s.len) {
                return .match;
            } else {
                return .no_match;
            }
        }

        // if escaped is true match
        // pattern character literally
        if (escaped) {
            const sc = sr.next();
            if (sc == 0) {
                return .no_match;
            }

            if (sc != pc) {
                return .no_match;
            }

            escaped = false;
            continue;
        }

        if (pc == '?') {
            // match any character
            if (sr.next() == 0) return .no_match;
            continue;
        }

        if (pc == '\\') {
            // escaped char
            escaped = true;
            continue;
        }

        if (pc == '[') {
            const sc = sr.next();
            if (sc == 0) {
                return .no_match;
            }

            const start_pos = pr.pos;
            var end_pos = start_pos;

            while (pc != ']') {
                pc = pr.next();
                if (pc == 0) {
                    return .no_match;
                }
                end_pos += 1;
            }

            const chars = pr.s[start_pos .. end_pos - 1];

            if (chars.len == 0) {
                return .no_match;
            }

            // match one of these characters
            for (chars) |c| {
                if (c == sc) {
                    continue :outer;
                }
            }
            return .no_match;
        }

        if (pc == '*') {
            // match zero or more characters
            pr.pos -= 1;
            const star_size = pr.read_star_pattern();
            if (star_size > sr.remaining()) {
                // not enough characters remaining in payload
                return .no_match;
            }

            // skip min number of chars
            sr.pos += star_size;

            const next_pc = pr.peek();

            if (next_pc == 0) {
                // there is a star at the end of the payload
                return .match;
            }

            // if at this point depth is 0 then we reached max depth of recursion, time to bail
            if (depth == 0) {
                return .recursion;
            }

            while (true) {
                if (!sr.skip_until(next_pc)) {
                    // could not find the next character in the payload so it's not a match
                    return .no_match;
                }

                // found next character, start another recursive match
                const ss = sr.s[sr.pos..];
                const sp = pr.s[pr.pos..];
                const match_result = globRecursively(sp, ss, depth - 1);

                // if substring was a match then return true
                // otherwise try to read more

                if (match_result == .no_match) {
                    sr.pos += 1;
                } else {
                    return match_result;
                }
            }
        }

        const sc = sr.next();
        if (sc != pc) {
            return .no_match;
        }
    }
}

const StringReader = struct {
    s: []const u8,
    pos: usize = 0,

    pub fn init(s: []const u8) StringReader {
        return .{ .s = s };
    }

    pub fn next(self: *StringReader) u8 {
        if (self.pos < self.s.len) {
            defer self.pos += 1;
            return self.s[self.pos];
        }
        return 0;
    }

    pub inline fn skip_until(self: *StringReader, c: u8) bool {
        while (true) {
            const sc = self.next();
            if (sc == 0) return false;
            if (sc == c) {
                self.pos -= 1;
                return true;
            }
        }

        return false;
    }

    pub fn peek(self: *StringReader) u8 {
        if (self.pos < self.s.len) {
            return self.s[self.pos];
        } else {
            return 0;
        }
    }

    pub inline fn remaining(self: *const StringReader) usize {
        return self.s.len - self.pos;
    }

    pub fn read_star_pattern(self: *StringReader) usize {
        var size: usize = 0;

        while (true) {
            const c = self.next();

            if (c == '*') {
                continue;
            } else if (c == '?') {
                size += 1;
            } else if (c == 0) {
                break;
            } else {
                self.pos -= 1;
                break;
            }
        }

        return size;
    }
};

test "glob: simple" {
    try std.testing.expect(glob("abc", "abc"));
    try std.testing.expect(!glob("abc", "ab"));
}

test "glob: ?" {
    try std.testing.expect(glob("???", "1W+"));
    try std.testing.expect(!glob("??", "1W+"));
}

test "glob: *" {
    try std.testing.expect(glob("*", "test"));
    try std.testing.expect(glob("*", "1"));
    try std.testing.expect(glob("*", "lorem ipsum hi there bro!"));
}

test "glob: * (complex, one per pattern)" {
    try std.testing.expect(glob("test.*", "test.123"));
    try std.testing.expect(!glob("test.*", "test1123"));
}

test "glob: * (complex, multiple per pattern)" {
    try std.testing.expect(glob("test.*.lol.*.last", "test.123.lol.456.last"));
}

test "glob: * (complex, multiple per pattern + ?)" {
    try std.testing.expect(glob("test.???*.lol.*.last", "test.123.lol.456.last"));
    try std.testing.expect(!glob("test.??*??.lol.*.last", "test.123.lol.456.last"));
}

test "glob: []" {
    try std.testing.expect(glob("[a]", "a"));
    try std.testing.expect(!glob("[a]", "b"));
    try std.testing.expect(glob("[abcdefg]", "g"));
}
