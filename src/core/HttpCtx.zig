//! An HTTP context object.
//! Given for each request and gets reused internally.

const std = @import("std");
const mem = std.mem;
const assert = std.debug.assert;
const io = @import("../io.zig");
const LinkedBuffer = @import("../io/LinkedBuffer.zig");

const HttpCtx = @This();
/// Owner of this context, I/O operations are done on this.
socket: io.Socket,
/// Pointer to event loop to execute I/O operations.
loop: *io.Loop,
/// Various flags of the context.
flags: packed struct {
    /// Whether or not `Connection: keep-alive` is set.
    /// We mostly expect requests with keep-alive in HTTP/1.1.
    persistent: bool = false,
    /// Whether or not `Transfer-Encoding: chunked` is set.-
    chunked: bool = false,
    /// Whether or not `recv_c` is queued for read operations.
    receiving: bool = false,
    /// Indicates if the user want to upgrade the protocol.
    upgrade: enum(u2) { none, http2, websocket, __rsv } = .none,
    /// Reserved for future usage.
    /// We try as much as we can to fit flags in a single byte.
    __rsv: u2 = 0,
} = .{},
/// HTTP status to send.
/// Initially set to `200 OK`.
status: std.http.Status = .ok,
/// Completion used for receiving data for this context.
/// We don't queue multiple receive operations currently since it'd make the code more complex.
recv_c: io.Completion = .{},
/// Allocator owned by this context.
arena: std.heap.ArenaAllocator,
/// At the end of this struct, we store a receive buffer that's used for receiving HTTP requests.
/// This is the capacity of it.
buffer_cap: u32,
/// This is the length of it.
buffer_len: u32 = 0,
// Buffer is stored here implicitly, call `.buffer()` to get it.
// buffer: [*]u8,

/// If this is set, caller cannot send `Content-Length` header.
/// If this is not set, caller cannot send `Transfer-Encoding` header.
pub inline fn isChunked(ctx: *const HttpCtx) bool {
    return ctx.flags.chunked;
}

/// Returns true if `Connection: keep-alive` is set.
pub inline fn isKeepAlive(ctx: *const HttpCtx) bool {
    return ctx.flags.persistent;
}

/// Returns true if the context is currently writing data to it's socket.
pub inline fn isFlushing(ctx: *const HttpCtx) bool {
    return ctx.flags.flushing;
}

/// Internal function.
/// Returns a pointer to start of trailing buffer.
pub inline fn bufferPtr(ctx: *HttpCtx) [*]align(@alignOf(HttpCtx)) u8 {
    const base_ptr: [*]align(@alignOf(HttpCtx)) u8 = @ptrCast(ctx);
    return base_ptr + @sizeOf(HttpCtx);
}

/// Internal function.
/// Returns the read buffer owned by this context.
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
/// Queues the `recv_c` completion for recv operation.
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
    // Make sure we don't queue the `recv_c` while it's already used.
    // If this assertion fails, there's definitely a bug.
    assert(ctx.flags.receiving == false);

    ctx.loop.recv(&ctx.recv_c, HttpCtx, ctx, ctx.socket, buf, comptime struct {
        /// Idea here is separating I/O related errors from protocol related errors.
        /// Caller should only focus on errors related to protocol implementation.
        fn intercept(
            _ctx: *HttpCtx,
            _: *io.Loop,
            _: *io.Completion,
            _: io.Socket,
            _buffer: []u8,
            result: io.RecvError!usize,
        ) void {
            // `recv_c` is free to use.
            _ctx.flags.receiving = false;

            const len = result catch |err| switch (err) {
                error.EndOfStream => unreachable, // FIXME: deprecated: we should allow 0 length reads
                error.WouldBlock => unreachable, // MUST be handled by io package
                error.SystemResources => @panic("NYI"), // FIXME: NYI
                error.ConnectionRefused, error.ConnectionResetByPeer, error.ConnectionTimedOut => {
                    // FIXME: NYI
                    @panic("NYI");
                },
                error.SocketNotConnected => unreachable, // `loop.recv` must be called after the socket connected
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

    ctx.flags.receiving = true;
}

pub const WriteError = mem.Allocator.Error;
pub const Writer = std.io.GenericWriter(*HttpCtx, WriteError, write);
/// Capacity of LinkedBuffer can be as low as LOW_WATERMARK.
pub const LOW_WATERMARK = 4096;

/// FIXME: Currently this function can write as much as max. u32.
/// Internal function.
/// Prefer `Writer` interface via `.writer()` instead.
///
/// Creates `Write` objects for given bytes and queues them.
/// Returns how many bytes written.
pub fn write(ctx: *HttpCtx, bytes: []const u8) WriteError!usize {
    // Total number of written bytes from `bytes` so far.
    var written: usize = 0;

    // If we already have a buffer in tail, we can use it.
    if (ctx.send_q.tail) |buf| {
        const len = buf.pushBytes(bytes);
        // If we have written everything, we're done here.
        if (len == bytes.len) {
            return len;
        }

        // Advance write count.
        written += len;
    }

    const remaining = bytes.len - written;

    // Create a new buffer to write remaining bytes.
    const buf = try LinkedBuffer.create(ctx.arena.allocator(), @max(LOW_WATERMARK, remaining));
    const len = buf.pushBytes(bytes[written..]);
    written += len;

    // Queue up.
    ctx.send_q.push(buf);
    return written;
}

/// Writer interface for this context.
/// This allows writing directly, other writer utilities such as `writeStatus` should be preferred over it.
pub fn writer(ctx: *HttpCtx) Writer {
    return Writer{ .context = ctx };
}

/// Internal function.
/// Flushes buffers queued in send queue.
pub fn flushSendQueue(
    ctx: *HttpCtx,
    /// Invoked when everything in the send queue has been flushed.
    /// If the send queue is filled after this invoked, another call to flush is needed.
    comptime on_flush_end: *const fn (ctx: *HttpCtx) void,
) void {
    // If we're already flushing, do nothing.
    if (ctx.isFlushing()) {
        return;
    }

    // Check if we have any buffers to send.
    if (ctx.send_q.pop()) |node| {
        // Get the slice we want to send.
        const buf = node.buffer()[0..node.buffer_len];

        // Queue a send operation.
        ctx.loop.send(.unlinked, &ctx.send_c, HttpCtx, ctx, ctx.socket, buf, comptime struct {
            fn interceptor(
                _ctx: *HttpCtx,
                _: *io.Loop,
                _: *io.Completion,
                _: io.Socket,
                _buf: []const u8,
            ) void {
                // TODO: error handling.
                // Flushing is finished.
                _ctx.flags.flushing = false;
                _ = _buf;

                // Invoke the callback.
                on_flush_end(_ctx);
            }
        }.interceptor);

        // Flushing is started.
        ctx.flags.flushing = true;
    }
}

pub fn flush(ctx: *HttpCtx) void {
    ctx.flushSendQueue(onFlushEnd);
}

fn onFlushEnd(ctx: *HttpCtx) void {
    _ = ctx;
    std.debug.print("everything sent!\n", .{});
}

pub inline fn setStatus(ctx: *HttpCtx, status: std.http.Status) void {
    ctx.status = status;
}

pub inline fn setHeader(ctx: *HttpCtx, key: []const u8, value: []const u8) mem.Allocator.Error!void {
    return ctx.headers.put(ctx.arena.allocator(), key, value);
}
