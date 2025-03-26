//! Single HTTP server instance.

const std = @import("std");
const mem = std.mem;
const posix = std.posix;
const io = @import("../io.zig");
const ContextPool = @import("ctx_pool.zig").ContextPool;
const HttpCtx = @import("HttpCtx.zig");
const Request = @import("Request.zig");

const allocator = std.heap.page_allocator;

/// Passed to each server instance.
const Handlers = struct {
    /// Invoked whenever a server receives a request.
    on_request: *const fn (ctx: *HttpCtx) anyerror!void,
    // In the future, we will have a handler for websockets.
    // In fact, we can add an handler for upgrade requests too.
    // on_ws: *const fn (ctx: *HttpCtx, wsc: *WebSocketCtx) void,
};

/// Intentionally generic, this way we can take comptime arguments in the future.
pub fn Server(comptime UserType: type, comptime handlers: Handlers) type {
    _ = UserType;

    return struct {
        const Self = @This();
        /// Listener socket.
        fd: io.Socket = io.invalid_socket,
        /// Whether or not fd is accepting connections at the moment.
        accepting: bool = false,
        /// Used by accept operations.
        accept_c: io.Completion = .{},
        /// Address of the server.
        addr: std.net.Address,
        /// Event loop owned by this instance.
        loop: io.Loop,

        /// Initializes a server.
        pub fn init(addr: std.net.Address) !Self {
            var loop = try io.Loop.init();
            errdefer loop.deinit();

            // On io_uring, we create direct descriptors table and register the server fd to ring.
            if (comptime io.backing == .io_uring) {
                // Create as much file descriptor as possible in the ring.
                // NOTE: We can increase the file descriptor count here too, via `setrlimit`.
                const rlimit = try posix.getrlimit(.NOFILE);

                const fd_table: u32 = @intCast(rlimit.max & std.math.maxInt(u32));
                try loop.directDescriptors(.sparse, fd_table);
            }

            return .{ .loop = loop, .addr = addr };
        }

        /// Deinitializes a server.
        pub fn deinit(server: *Self) void {
            server.loop.deinit();
        }

        /// Creates a file descriptor for accepting connections.
        fn createServerDescriptor(server: *Self, kernel_backlog: u31) !void {
            // On io_uring, register the server fd to ring.
            if (comptime io.backing == .io_uring) {
                // Create the listener fd.
                const fd = try server.loop.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
                errdefer posix.close(fd);
                // Allow binding again.
                try server.loop.setReuseAddr(fd);
                // Kernel level load balancing.
                try server.loop.setReusePort(fd);
                // Disable nagle's algorithm.
                try server.loop.setTcpNoDelay(fd, true);

                // Bind to address.
                try posix.bind(fd, &server.addr.any, server.addr.getOsSockLen());
                // Listen for connections.
                try posix.listen(fd, kernel_backlog);

                // NOTE: This part is important since it assigns the server fd to 0.
                // We can use fd 0 for server's I/O actions from now on.
                try server.loop.updateDescriptorsSync(0, &[1]io.Socket{fd});

                // Assign 0 as fd.
                server.fd = 0;
                // Free the userspace fd.
                posix.close(fd);
            } else {
                @compileError("createServerDescriptor is not implemented for this backend yet");
            }
        }

        /// Creates an arena allocator and `HttpCtx` for a given socket.
        fn createContext(server: *Self, socket: io.Socket) mem.Allocator.Error!*HttpCtx {
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            errdefer arena.deinit();

            const bytes = try arena.allocator().alignedAlloc(u8, @alignOf(HttpCtx), 4096);
            // Interpret first @sizeOf(HttpCtx) bytes as HttpCtx.
            const ctx: *HttpCtx = @ptrCast(bytes.ptr);

            // Initialize the context.
            ctx.* = .{
                .loop = &server.loop,
                .socket = socket,
                // The context holds it's arena intrusively.
                .arena = arena,
                .buffer_cap = 4096 - @sizeOf(HttpCtx),
            };

            return ctx;
        }

        /// Returns a pointer to server from pointer to loop.
        inline fn fromLoop(loop: *io.Loop) *Self {
            return @fieldParentPtr("loop", loop);
        }

        /// Run the server.
        pub fn run(server: *Self) !void {
            // Setup the server file descriptor.
            try server.createServerDescriptor(128);
            // Start accepting connections.
            server.accept(onClient);
            // Run the event loop.
            try server.loop.run();
        }

        /// Internal function.
        /// Queues the `accept_c` completion for accept operation.
        /// This can only be stopped via error or `cancelAccept`.
        /// This function also creates the `HttpCtx` required for the socket
        /// where caller owns the context.
        /// SAFETY: If already accepting, this function does nothing and returns.
        fn accept(server: *Self, comptime on_accept: *const fn (server: *Self, ctx: *HttpCtx) void) void {
            // If we're already accepting connections, do nothing.
            if (server.accepting) {
                return;
            }

            server.loop.accept(&server.accept_c, Self, server, server.fd, comptime struct {
                // Interceptor here handles the errors for us.
                // Not the protocol errors though, they should be handled in `on_accept`.
                fn intercept(
                    _server: *Self,
                    _: *io.Loop,
                    _: *io.Completion,
                    _: io.Socket,
                    result: io.AcceptError!io.Socket,
                ) void {
                    const socket = result catch |err| switch (err) {
                        error.WouldBlock => unreachable, // MUST be handled by io package
                        error.ConnectionAborted => @panic("NYI"), // FIXME: NYI
                        error.Cancelled => return, // operation cancelled
                        error.SocketNotListening => unreachable, // we're definitely listening
                        error.ProcessFdQuotaExceeded, error.SystemFdQuotaExceeded, error.SystemResources => {
                            // Complete serving connections that're currently processing since we don't have any slot left.
                            // We can continue accepting after an fd is closed.
                            _server.accepting = false;
                            return;
                        },
                        error.OperationNotSupported => unreachable, // we're on TCP
                        error.PermissionDenied => @panic("EPERM"),
                        error.ProtocolFailure => unreachable,
                        error.Unexpected => unreachable,
                    };

                    // Create a context for the socket.
                    const ctx = _server.createContext(socket) catch |err| switch (err) {
                        error.OutOfMemory => {
                            // We can't handle connections at the moment.
                            @panic("OOM");
                        },
                    };

                    @call(.always_inline, on_accept, .{ _server, ctx });
                }
            }.intercept);

            // We queued an accept operation.
            server.accepting = true;
        }

        /// Cancels accepting connections.
        /// TODO: impl
        fn cancelAccept(_: *Self) void {
            @panic("Not implemented yet");
        }

        /// Emitted whenever a new client drops by.
        fn onClient(_: *Self, ctx: *HttpCtx) void {
            // We start the HTTP cycle.
            ctx.read(ctx.buffer(), onRead);
        }

        /// Internal function.
        /// Emitted when there's HTTP request data, this will try to parse the HTTP request out of it.
        fn onRead(ctx: *HttpCtx, _: []u8, read_len: usize) void {
            // Advance as much as we've received.
            ctx.advance(@intCast(read_len));

            // Try to parse the HTTP request.
            var req = Request{};
            // `consumed` returns how many bytes request line (GET / HTTP/1.1) + headers consist of.
            const consumed = req.parse(ctx.buffer()[0..ctx.buffer_len]) catch |err| switch (err) {
                // Read more.
                error.Incomplete => return ctx.read(ctx.buffer(), onRead),
                error.Invalid => @panic("invalid request"), // FIXME: NYI
            };
            _ = consumed;

            // If we got here, request is parsed successfully.
            // We can invoke the user-provided `on_request`.
            handlers.on_request(ctx) catch {
                // FIXME: handle error
                @panic("NYI");
            };

            ctx.flush();
        }
    };
}
