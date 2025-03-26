const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const HttpCtx = @import("HttpCtx.zig");
const io = @import("../io.zig");

/// Pool of `HttpCtx`.
/// This allocates HTTP contexts with a trailing buffer at the end.
/// For more information, check `HttpCtx.zig`.
pub fn ContextPool(item_size: u32) type {
    assert(item_size > @sizeOf(HttpCtx));

    return struct {
        arena: std.heap.ArenaAllocator,
        /// Contexts owned by the pool.
        /// Whenever caller requests a new context, pool will first check here
        /// for a free allocation.
        ///
        /// Whenever caller destroys a context, it'll end up here rather than freed.
        ///
        /// Contexts are intrusively linked by their `next` field.
        free_list: ?*HttpCtx = null,

        const Self = @This();

        /// Initializes a new ContextPool.
        /// Each running worker thread will call this or `initPreheated` once.
        pub fn init(allocator: mem.Allocator) Self {
            return .{ .arena = std.heap.ArenaAllocator.init(allocator) };
        }

        /// Frees all resources that're created by this pool.
        pub fn deinit(pool: *Self) void {
            pool.arena.deinit();
        }

        /// Gives an unused context from the pool or allocates for a new one.
        /// This function also initializes the context.
        /// SAFETY: Ownership is handed over to caller.
        pub fn create(pool: *Self, loop: *io.Loop, socket: io.Socket) !*HttpCtx {
            const ctx = blk: {
                // We got an unused item.
                if (pool.free_list) |item| {
                    pool.free_list = item.next;
                    break :blk item;
                }

                // free_list is empty, allocate a context.
                // We intrusively allocate a context in a large buffer.
                const bytes = try pool.arena.allocator().alignedAlloc(u8, @alignOf(HttpCtx), item_size);

                // Interpret first @sizeOf(HttpCtx) bytes as HttpCtx.
                const ptr: *HttpCtx = @ptrCast(bytes.ptr);
                break :blk ptr;
            };

            // Initialize the context.
            ctx.* = .{
                .loop = loop,
                .socket = socket,
                .arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
                .buffer_cap = item_size - @sizeOf(HttpCtx),
            };

            return ctx;
        }

        /// Returns the ownership of a context back to buffer.
        /// Only the contexts that're created via `create` can be put back.
        pub inline fn put(pool: *Self, ctx: *HttpCtx) void {
            assert(ctx.next == null);
            // free_list works like a LIFO.
            ctx.next = pool.free_list;
            pool.free_list = ctx;
        }
    };
}
