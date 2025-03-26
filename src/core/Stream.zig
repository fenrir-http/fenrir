//! Stream represents a connection to a remote client.
//! It provides methods for reading and writing data over the connection.
//! Streams are not bounded to any specific protocol or format.

const std = @import("std");
const mem = std.mem;
const io = @import("../io.zig");
const LinkedBuffer = @import("../io/LinkedBuffer.zig");

const Stream = @This();
/// Pointer to event loop to execute I/O operations.
loop: *io.Loop,
/// Underlying socket of this stream.
socket: io.Socket,
/// Various flags of the stream, packed for space efficiency.
flags: packed struct {
    /// Indicates if the stream is currently writing data to it's socket.
    flushing: bool = false,
    /// Indicates if the stream is currently receiving data.
    is_receiving: bool = false,
    /// Reserved for future use.
    __rsv: u6 = 0,
} = .{},
/// Chain of buffers this context will write to it's socket.
/// This allows us to order & pipeline send operations.
send_q: LinkedBuffer.Queue = .{},
/// Completion used for receiving data.
/// We don't allow queuing multiple receive operations currently since it'd make the code more complex.
recv_c: io.Completion = .{},
/// Completion used for sending data.
send_c: io.Completion = .{},

pub const WriteError = mem.Allocator.Error;
/// Capacity of LinkedBuffer can be as low as LOW_WATERMARK.
/// TODO: We can add HIGH_WATERMARK here too.
pub const LOW_WATERMARK = 4096;

/// FIXME: Currently this function can write as much as max. u32.
///
/// Creates `Write` objects for given bytes and queues them.
/// To actually put data on the wire, a call to `flush` is required.
/// Returns how many bytes queued.
pub fn write(stream: *Stream, allocator: mem.Allocator, bytes: []const u8) WriteError!usize {
    const buf_len: u32 = @intCast(bytes.len);
    // Total number of written bytes from `bytes` so far.
    var written: u32 = 0;

    // If we already have a buffer in tail, we can use it.
    if (stream.send_q.tail) |buf| {
        const len = buf.pushBytes(bytes);
        // If we have written everything, we're done here.
        if (len == buf_len) {
            return buf_len;
        }

        // Advance write count.
        written += len;
    }

    const remaining = buf_len - written;

    // Create a new buffer to write remaining bytes.
    const buf = try LinkedBuffer.create(allocator, @max(LOW_WATERMARK, remaining));
    _ = buf.pushBytes(bytes[written..]);

    // Queue up.
    stream.send_q.push(buf);
    return written;
}

/// Returns true if the stream is currently writing data to it's socket.
pub inline fn isFlushing(stream: *const Stream) bool {
    return stream.flags.flushing;
}

/// Flushes buffers queued in send queue.
pub fn flush(
    stream: *Stream,
    /// Invoked when everything in the send queue has been flushed.
    /// If the send queue is filled after this invoked, another call to flush is needed.
    comptime on_flush_end: *const fn () void,
) !void {
    // If we're already flushing, do nothing.
    if (stream.isFlushing()) {
        return;
    }

    // Check if we have any buffers to send.
    if (stream.send_q.pop()) |node| {
        // Get the slice we want to send.
        const buffer = node.buffer()[0..node.buffer_len];

        // Queue a send operation.
        stream.loop.send(.unlinked, &stream.send_c, Stream, stream, stream.socket, buffer, comptime struct {
            fn interceptor(
                _stream: *Stream,
                _: *io.Loop,
                _: *io.Completion,
                _: io.Socket,
                _: []const u8,
            ) void {
                _ = _stream;
                // TODO: error handling.

                // Invoke the callback when done.
                on_flush_end();
            }
        }.interceptor);

        // Set the flushing flag.
        stream.flags.flushing = true;
    }
}
