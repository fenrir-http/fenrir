const std = @import("std");
const mem = std.mem;
// NOTE: We depend on io with presets, this may change in the future.
const io = @import("../io.zig");
const Intrusive = @import("../queue.zig").Intrusive;

const LinkedBuffer = @This();
/// Queue type for buffers to push/pull from.
pub const Queue = Intrusive(LinkedBuffer);
/// Intrusively linked to other buffers.
next: ?*LinkedBuffer = null,
/// Capacity of the trailing buffer.
buffer_cap: usize,
/// Length of the trailing buffer.
buffer_len: usize = 0,
/// Completion for submitting this `Write` for io.
/// Disabled for now.
//completion: io.Completion = .{},
// NOTE: Buffer with size of `buffer_cap` is stored here implicitly, call `.buffer()` to get it.
// buffer: [*]u8,

/// Creates a new LinkedBuffer.
/// LinkedBuffer itself only owns the allocation, not the allocator.
pub fn create(allocator: mem.Allocator, cap: usize) mem.Allocator.Error!*LinkedBuffer {
    const bytes = try allocator.alignedAlloc(u8, @alignOf(LinkedBuffer), cap + @sizeOf(LinkedBuffer));
    // Initialize the Write.
    const ptr: *LinkedBuffer = @ptrCast(bytes.ptr);
    ptr.* = .{ .buffer_cap = cap };

    return ptr;
}

/// Returns a pointer to start of trailing buffer.
pub inline fn bufferPtr(w: *LinkedBuffer) [*]align(@alignOf(LinkedBuffer)) u8 {
    const base_ptr: [*]align(@alignOf(LinkedBuffer)) u8 = @ptrCast(w);
    return base_ptr + @sizeOf(LinkedBuffer);
}

/// Returns the read buffer owned by this Write.
pub inline fn buffer(w: *LinkedBuffer) []u8 {
    return w.bufferPtr()[0..w.buffer_cap];
}

/// Returns how many bytes buffer can take currently.
pub inline fn remainingCapacity(w: *const LinkedBuffer) usize {
    return w.buffer_cap - w.buffer_len;
}

/// Returns a slice to available buffer.
/// SAFETY: slice may be 0 length.
pub inline fn availableBuffer(w: *LinkedBuffer) []u8 {
    return (w.bufferPtr() + w.buffer_len)[0..w.remainingCapacity()];
}

/// Advances the buffer length by given value.
pub inline fn advance(w: *LinkedBuffer, by: usize) void {
    w.buffer_len += by;
}

/// Pushes the bytes to the end of the `Write`.
/// It's not guaranteed to push the whole buffer, returns how many bytes are pushed successfully.
pub inline fn pushBytes(w: *LinkedBuffer, bytes: []const u8) usize {
    // We can push only as much as remaining capacity.
    const len = @min(w.remainingCapacity(), bytes.len);
    // Copy to internal buffer.
    @memcpy(w.availableBuffer(), bytes.ptr);
    // Advance.
    w.advance(len);
    return len;
}

/// Pushes the bytes to the end of the `Write`.
/// SAFETY: The caller must ensure that the buffer has enough capacity.
pub inline fn pushAssumeCapacity(w: *LinkedBuffer, bytes: []const u8) void {
    @memcpy(w.availableBuffer()[0..bytes.len], bytes.ptr);
    w.advance(bytes.len);
}

/// Reserves a slice in a desired length from the end of the buffer.
/// Advances as much as given length.
/// SAFETY: The caller must ensure that the buffer has enough capacity.
pub inline fn reserve(w: *LinkedBuffer, len: usize) []u8 {
    const slice = w.availableBuffer()[0..len];
    w.advance(len);
    return slice;
}
