//! An HTTP context object.
//! Given for each request and gets reused internally.

const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const io = @import("../io.zig");

const HttpCtx = @This();
/// Owner of this context, I/O operations are done on this.
socket: io.Socket,
/// Pointer to event loop to execute I/O operations.
loop: *io.Loop,
/// Completion used for receiving data for this context.
/// We don't queue multiple receive operations but rather rely on this here.
read_c: io.Completion = .{},
/// Allocator owned by this context.
arena: std.heap.ArenaAllocator,
/// At the end of this struct, we store a receive buffer that's used for receiving HTTP requests.
/// This is the capacity of it.
buffer_cap: u32,
/// This is the length of it.
buffer_len: u32 = 0,
// Buffer is stored here implicitly, call `.buffer()` to get it.
// buffer: [*]u8,

/// Internal function.
/// Returns a pointer to start of trailing buffer.
pub inline fn bufferPtr(ctx: *HttpCtx) [*]align(@alignOf(HttpCtx)) u8 {
    const base_ptr: [*]align(@alignOf(HttpCtx)) u8 = @ptrCast(ctx);
    return base_ptr + @sizeOf(HttpCtx);
}

/// Internal function.
/// Returns the whole read buffer owned by this context.
pub inline fn buffer(ctx: *HttpCtx) []u8 {
    return ctx.bufferPtr()[0..ctx.buffer_cap];
}

/// Internal function.
/// Returns a slice to buffer starting from the consumable length.
/// (buffer_ptr + buffer_len)[0 .. buffer_cap - buffer_len]
pub inline fn bufferAvailable(ctx: *HttpCtx) []u8 {
    return (ctx.bufferPtr() + ctx.buffer_len)[0 .. ctx.buffer_cap - ctx.buffer_len];
}

/// Internal function.
/// Advances the buffer length by given value.
pub inline fn advance(ctx: *HttpCtx, by: u32) void {
    ctx.buffer_len += by;
}

/// Internal function.
/// Queues the `recv_c` for recv operation.
pub fn read(
    ctx: *HttpCtx,
    buf: []u8,
    /// Invoked when the operation is completed successfully.
    comptime on_read: *const fn (
        /// Context that owns this call.
        ctx: *HttpCtx,
        /// Buffer given to read function.
        buf: []u8,
        /// Result of `loop.recv` but errors are handled via interceptor.
        read_len: usize,
    ) void,
) void {
    return ctx.loop.recv(&ctx.read_c, HttpCtx, ctx, ctx.socket, buf, 0, comptime struct {
        /// Idea here is separating I/O related errors from protocol related errors.
        /// Caller should only focus on errors related to protocol implementation.
        fn intercept(
            _ctx: *HttpCtx,
            _: *io.Loop,
            _: *io.Completion,
            _buffer: []u8,
            result: io.RecvError!usize,
        ) void {
            const len = result catch |err| switch (err) {
                error.WouldBlock => unreachable, // MUST be handled by io package
                error.SystemResources => @panic("NYI"), // FIXME: NYI
                error.ConnectionRefused, error.ConnectionResetByPeer, error.ConnectionTimedOut => {
                    // FIXME: NYI
                    @panic("NYI");
                },
                error.SocketNotConnected => unreachable, // must be called after the socket connected
                error.OperationNotSupported => unreachable, // backing protocol must support recv
                error.PermissionDenied => @panic("EPERM"),
                error.Cancelled => {
                    // FIXME: NYI
                    @panic("NYI");
                },
                error.Unexpected => unreachable, // unexpected error
            };

            // Call provided callback.
            @call(.always_inline, on_read, .{ _ctx, _buffer, len });
        }
    }.intercept);
}
