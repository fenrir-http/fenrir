const std = @import("std");
const mem = std.mem;
const posix = std.posix;
const io = @import("../io.zig");
const HttpCtx = @import("HttpCtx.zig");
const Request = @import("Request.zig");

const log = std.log.scoped(.@"http-worker");

/// Various options for an instance.
pub const Tuning = struct {
    /// Total size of memory owned by a single server instance.
    /// Instances allocate their memory when they're initialized and never ask system for more.
    /// Default memory is 16Mb.
    memory_size: usize = 1024 * 1024 * 16,
};

/// Passed to each server instance.
pub const Handlers = struct {
    /// Invoked whenever a server receives a request.
    on_request: *const fn (ctx: *HttpCtx) anyerror!void,
    // In the future, we will have a handler for websockets.
    // In fact we can add an handler for upgrade requests too.
};

const Arena = std.heap.ArenaAllocator;
const Fba = std.heap.FixedBufferAllocator;

/// Instances are simply decoupled runners of our server.
/// Each instance owns it's own event loop, memory and listener socket.
pub fn InstanceImpl(comptime tuning: Tuning, comptime handlers: Handlers) type {
    _ = handlers;

    return struct {
        const Instance = @This();
        /// Event loop owned by this instance.
        loop: io.Loop,
        /// Listener socket owned by this instance.
        /// We never share listener sockets; each thread do their own listening thanks to SO_REUSEPORT.
        listener: io.Socket = io.invalid_socket,
        /// Used by accept operations.
        accept_c: io.Completion = .{},
        /// Active count of connections.
        conns: usize = 0,
        /// Heap memory owned by this instance.
        fba: Fba,

        /// Initializes a server instance.
        pub fn init(allocator: mem.Allocator) !Instance {
            const memory = try allocator.alloc(u8, tuning.memory_size);
            return .{ .loop = try io.Loop.init(), .fba = .init(memory) };
        }

        /// Returns a pointer to instance from pointer to loop.
        inline fn fromLoop(loop: *io.Loop) *Instance {
            return @fieldParentPtr("loop", loop);
        }

        /// Intrusively allocates an `HttpCtx` for a given socket.
        fn createContext(instance: *Instance, socket: io.Socket) mem.Allocator.Error!*HttpCtx {
            var arena = std.heap.ArenaAllocator.init(instance.fba.allocator());
            errdefer arena.deinit();

            const bytes = try arena.allocator().alignedAlloc(u8, @alignOf(HttpCtx), 4096 + @sizeOf(HttpCtx));
            // Interpret first @sizeOf(HttpCtx) bytes as HttpCtx.
            const ctx: *HttpCtx = @ptrCast(bytes.ptr);

            // Initialize the context.
            ctx.* = .{
                .loop = &instance.loop,
                .socket = socket,
                .arena = arena,
                .buffer_cap = 4096,
            };

            return ctx;
        }

        /// Internal function.
        /// Queues the `accept_c` for accept operation. This can only be stopped via error or `cancelAccept`.
        /// This function also creates the `HttpCtx` required for the socket where caller owns the context.
        /// # SAFETY:
        /// If already accepting, this function does nothing and returns.
        inline fn accept(instance: *Instance, comptime on_accept: *const fn (instance: *Instance, ctx: *HttpCtx) void) void {
            return instance.loop.accept(&instance.accept_c, Instance, instance, instance.listener, comptime struct {
                // Interceptor here handles the IO-level errors for us.
                // Not protocol errors though, they should be handled in `on_accept`.
                fn intercept(
                    _inst: *Instance,
                    _: *io.Loop,
                    _: *io.Completion,
                    result: io.AcceptError!io.Socket,
                ) void {
                    const socket = result catch |err| switch (err) {
                        error.WouldBlock => unreachable, // MUST be handled by io package
                        error.ConnectionAborted => @panic("NYI"), // FIXME: NYI
                        error.Cancelled => return, // operation cancelled
                        error.SocketNotListening => unreachable, // we're definitely listening
                        error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded, error.SystemResources => {
                            // TODO: Complete serving connections that're currently processing since we don't have any slot left.
                            // We can continue accepting after an fd is closed.
                            @panic("OOM");
                        },
                        error.OperationNotSupported => unreachable, // underlying protocol must support accept operations
                        error.PermissionDenied => unreachable,
                        error.ProtocolFailure => unreachable,
                        error.Unexpected => unreachable,
                    };

                    // Create a context for the socket.
                    const ctx = _inst.createContext(socket) catch |err| switch (err) {
                        error.OutOfMemory => {
                            // We can't handle connections at the moment.
                            @panic("OOM");
                        },
                    };

                    @call(.auto, on_accept, .{ _inst, ctx });
                }
            }.intercept);
        }

        fn onAccept(instance: *Instance, ctx: *HttpCtx) void {
            // Increase HTTP connection count.
            instance.conns += 1;

            // HTTP transaction starts.
            // Read the next request.
            ctx.read(ctx.buffer(), onRequestInitRead);
        }

        /// # SAFETY:
        /// 0 reads are not interpretted as errors; should be handled here.
        fn onRequestInitRead(ctx: *HttpCtx, _: []u8, read_len: usize) void {
            // Check if other side is closed.
            if (read_len == 0) {
                @panic("peer is closed");
            }

            // Increment the buffer length as much as read.
            ctx.buffer_len += @intCast(read_len);

            const whole = ctx.bufferPtr()[0..ctx.buffer_len];
            // Try to parse an HTTP request.
            var req = Request{};
            const req_len = req.parse(whole) catch |err| switch (err) {
                error.Incomplete => {
                    const remaining = ctx.bufferAvailable();
                    // Request is larger than we can handle; we reject accepting requests > 4096 bytes.
                    if (remaining.len == 0) {
                        @panic("this should be handled as invalid too");
                    }

                    return ctx.read(remaining, onRequestInitRead);
                },
                error.Invalid => @panic("handle invalid request"),
            };
            _ = req_len;

            log.info("(thread-{}) {s} {?s}", .{ std.Thread.getCurrentId(), @tagName(req.method), req.path });

            // Check if we can keep alive this request.
            std.debug.print("{}\n", .{req.isKeepAlive()});
        }

        /// Starts serving connections.
        pub fn run(instance: *Instance, port: u16) !void {
            // Create our listener socket.
            const listener = try posix.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
            errdefer posix.close(listener);

            const addr = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, port);
            try instance.loop.setReuseAddr(listener);
            try instance.loop.setReusePort(listener);
            try instance.loop.setTcpNoDelay(listener, true);
            try posix.bind(listener, &addr.any, addr.getOsSockLen());
            try posix.listen(listener, 128);

            instance.listener = listener;

            // Queue the accept_c.
            instance.accept(onAccept);

            log.info("(thread-{}) listening for connections", .{std.Thread.getCurrentId()});
            // Run event loop.
            return instance.loop.run();
        }
    };
}
