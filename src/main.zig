const std = @import("std");
const Server = @import("core/server.zig").Server;
const HttpCtx = @import("core/HttpCtx.zig");
const io = @import("io.zig");

pub fn main() !void {
    var loop = try io.Loop.init();
    defer loop.deinit();

    //var bp = try io.BufferPool.init(std.heap.page_allocator, &loop, 1, 10, 32);
    //defer bp.deinit(std.heap.page_allocator, &loop);

    //var fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, std.posix.IPPROTO.TCP);

    //const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080);
    //try std.posix.bind(fd, &addr.any, addr.getOsSockLen());
    //try std.posix.listen(fd, 128);
    //try loop.setReuseAddr(fd);

    //try loop.directDescriptors(.sparse, 1024);
    //try loop.updateDescriptorsSync(0, &.{fd});
    //std.posix.close(fd);

    //fd = 0;

    try loop.run();

    //var server = try Server(void, .{
    //    .on_request = onRequest,
    //}).init(std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 8080));
    //defer server.deinit();

    //var state = std.heap.ArenaAllocator.State{};
    //
    //const BufferList = std.SinglyLinkedList(usize);
    //const BufNode = BufferList.Node;
    //const BufNode_alignment: std.mem.Alignment = .fromByteUnits(@alignOf(BufNode));
    //
    //const actual_min_size = 8192 + (@sizeOf(BufNode) + 16);
    //const big_enough_len = 0 + actual_min_size;
    //const len = big_enough_len + big_enough_len / 2;
    //const ptr = std.heap.page_allocator.rawAlloc(len, BufNode_alignment, @returnAddress()) orelse unreachable;
    //const buf_node: *BufNode = @ptrCast(@alignCast(ptr));
    //buf_node.* = .{ .data = len };
    //state.buffer_list.prepend(buf_node);
    //state.end_index = 0;
    //
    //var arena = state.promote(std.heap.page_allocator);
    //
    //const allocator = arena.allocator();
    //
    //const first = state.buffer_list.popFirst() orelse unreachable;
    //std.debug.print("{}\n", .{first});

    //try server.run();
}

fn onRequest(ctx: *HttpCtx) !void {
    _ = ctx;
    //ctx.setStatus(.ok);
    //try ctx.setHeader("Content-Type", "text/html");

    //const writer = ctx.bodyWriter();

    // Write some body.
    //try writer.writeAll("<html><body><h1>Hello, World!</h1></body></html>");
}

test {
    _ = @import("io.zig");
}
