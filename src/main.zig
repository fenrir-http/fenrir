const std = @import("std");
const Instance = @import("core/server.zig").InstanceImpl;
const HttpCtx = @import("core/HttpCtx.zig");
const io = @import("io.zig");

pub fn main() !void {
    var instance = try Instance(.{}, .{
        .on_request = onRequest,
    }).init(std.heap.page_allocator);

    try instance.run(8080);
}

pub fn onRequest(ctx: *HttpCtx) !void {
    _ = ctx;
}
