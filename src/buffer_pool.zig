const std = @import("std");
const mem = std.mem;
const heap = std.heap;

/// A pool of buffers that reuse the same buffer.
/// Since buffers get reused, its faster to allocate this way.
///
/// SAFETY: The buffer pool is not thread-safe, either use it only in a thread
/// or access it via some kind of mutex/lock.
pub fn BufferPool(comptime buffer_len: usize) type {
    return struct {
        const Self = @This();

        /// A single buffer object.
        const Buffer = struct {
            /// Pointer to next Buffer, if there's none, set to `null`.
            next: ?*Buffer = null,
            /// Tracks how much space of this buffer is used.
            len: usize = 0,
            /// Bytes.
            buffer: [buffer_len - (@sizeOf(*Buffer) + @sizeOf(usize))]u8 = undefined,
        };

        /// Arena allocator that we use to create buffers.
        arena: heap.ArenaAllocator,
        /// Buffers that are only owned by buffer pool.
        free_list: ?*Buffer = null,

        /// Initialize a new buffer pool.
        pub fn init(allocator: mem.Allocator) Self {
            return .{ .arena = heap.ArenaAllocator.init(allocator) };
        }

        /// Destroys the pool and frees all buffers.
        /// This frees both owned and non-owned buffers.
        pub inline fn deinit(self: *Self) void {
            self.arena.deinit();
        }

        /// Hand-overs a buffer from the buffer pool.
        /// If there are no buffers in the pool, it allocates a new one.
        pub fn create(self: *Self) !*Buffer {
            const buffer = blk: {
                // Hand-over the buffer on top of the free list.
                if (self.free_list) |buf| {
                    self.free_list = buf.next;
                    break :blk buf;
                }

                // Allocate a new buffer since we don't have any.
                break :blk self.arena.allocator().create(Buffer);
            };

            // Initialize the buffer with default params.
            buffer.* = .{};

            return buffer;
        }

        /// Returns a buffer back to buffer pool.
        /// SAFETY: After this function is called, buffer is owned by the pool.
        pub inline fn destroy(self: *Self, buffer: *Buffer) void {
            buffer.next = self.free_list;
            self.free_list = buffer;
        }
    };
}
