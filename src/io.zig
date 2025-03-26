//! Returns async I/O primitives depending on the platform.

const std = @import("std");
const os = std.os;
const windows = os.windows;
const builtin = @import("builtin");

pub const IoEngine = @import("io/options.zig").IoEngine;

/// Returns the backing I/O engine.
pub const backing: IoEngine = switch (builtin.os.tag) {
    .linux => .io_uring,
    .windows => .iocp,
    else => @compileError("I/O engine for this platform is not implemented yet"),
};

/// Asynchronous event loop implementation.
const loop = switch (builtin.os.tag) {
    .linux => @import("io/io_uring.zig"),
    else => @compileError("I/O engine for this platform is not implemented yet"),
};

/// Default loop type used in everywhere.
pub const Loop = loop.Loop(.{
    .io_uring = .{
        .direct_descriptors_mode = true,
        .zero_copy_sends = false,
    },
});

/// FIXME: usingnamespace might get snapped away from existence.
pub usingnamespace Loop;

/// Invalid socket. Can be given initially.
pub const invalid_socket = switch (builtin.os.tag) {
    .windows => windows.ws2_32.INVALID_SOCKET,
    else => -1,
};

/// Invalid handle. Can be given initially.
pub const invalid_handle = switch (builtin.os.tag) {
    .windows => windows.INVALID_HANDLE_VALUE,
    else => -1,
};

const BufferPoolGeneric = @import("io/buffer_pool.zig").BufferPool;

/// Buffer pool that can be used by loop.
pub const BufferPool = switch (builtin.os.tag) {
    .linux => BufferPoolGeneric(.io_uring),
    .windows => BufferPoolGeneric(.iocp),
    else => @compileError("Buffer pool for this platform is not implemented yet"),
};

test {
    _ = switch (builtin.os.tag) {
        .linux => @import("io/io_uring.zig"),
        else => unreachable,
    };
}
