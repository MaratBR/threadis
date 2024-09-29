const std = @import("std");

pub fn MkCTMap(comptime value: anytype) type {
    const type_info = @typeInfo(@TypeOf(value));
    return switch (type_info) {
        .Struct => {
            const struct_info = type_info.Struct;
            const n = struct_info.fields.len;

            if (n < 1) {
                @compileError("MkCTMap expects struct to have at least one field");
            }

            const value_type: type = struct_info.fields[0].type;

            for (1..n) |i| {
                if (value_type != struct_info.fields[i].type) {
                    @compileError("MkCTMap expects all fields to be the same type");
                }
            }

            var values: [n]value_type = undefined;
            var keys: [n][]const u8 = undefined;

            for (0..n) |i| {
                keys[i] = struct_info.fields[i].name;
                values[i] = @field(value, struct_info.fields[i].name);
            }

            return CTMap(n, keys, value_type, values);
        },
        else => {
            @compileError("MkCTMap expects type argument to be a struct");
        },
    };
}

pub fn CTMap(comptime n: comptime_int, comptime keys: [n][]const u8, comptime T: type, comptime values: [n]T) type {
    return CTMapHash(n, keys, T, values, std.hash.Wyhash);
}

fn CTMapHash(comptime n: comptime_int, comptime keys: [n][]const u8, comptime T: type, comptime values: [n]T, comptime hash_type: type) type {
    if (n < 1) {
        @compileError("n must be 1 or higher");
    }

    var entries: [n]T = undefined;
    var entries_hash: [n]u64 = undefined;
    const min_hash = blk: {
        var min_hash_v: u64 = std.math.maxInt(u64);

        for (0..n) |i| {
            entries_hash[i] = hash_type.hash(0, keys[i]);
            entries[i] = values[i];
            if (entries_hash[i] < min_hash_v) {
                min_hash_v = entries_hash[i];
            }
        }

        break :blk min_hash_v;
    };

    for (0..n) |i| {
        entries_hash[i] -= min_hash;
    }

    const eas = blk: {
        var eas: usize = n;

        while (true) {
            var arr_indices: [n]usize = undefined;

            for (0..n) |i| {
                arr_indices[i] = entries_hash[i] % eas;
            }

            if (!hasDuplicates(usize, &arr_indices)) {
                break;
            } else {
                eas += 1;
            }
        }

        break :blk eas;
    };

    if (eas > 1024) {
        @compileError("CTMap is too big");
    }

    const arr: [eas]T = blk: {
        var arr_v: [eas]T = undefined;

        for (0..n) |i| {
            arr_v[entries_hash[i] % eas] = entries[i];
        }

        break :blk arr_v;
    };

    return struct {
        pub const ValueType = T;

        pub fn get(key: []const u8) ?T {
            const hash = hash_type.hash(0, key);
            if (hash < min_hash) return null;
            const idx = (hash - min_hash) % eas;
            if (idx >= arr.len) {
                return null;
            }
            return arr[idx];
        }
    };
}

fn hasDuplicates(comptime T: type, arr: []T) bool {
    for (0..arr.len) |i| {
        const v = arr[i];
        @setEvalBranchQuota(10000);
        for (i + 1..arr.len) |j| {
            const v2 = arr[j];
            if (v2 == v) {
                return true;
            }
        }
    }

    return false;
}
