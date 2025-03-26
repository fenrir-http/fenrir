// Code here is heavily based on both tigerbeetle/io and libxev.
// Thanks TigerBeetle team and @mitchellh!
// https://github.com/tigerbeetle/tigerbeetle/blob/main/src/io/linux.zig
// https://github.com/mitchellh/libxev/blob/main/src/backend/io_uring.zig

const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const posix = std.posix;
const linux = os.linux;
const IO_Uring = linux.IoUring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;
const Intrusive = @import("../queue.zig").Intrusive;
const Options = @import("options.zig").Options;
const BufferPool = @import("buffer_pool.zig").BufferPool(.io_uring);
const ex = @import("linux/io_uring_ex.zig");

const log = std.log.scoped(.loop);

/// Event notifier implementation based on io_uring.
/// FIXME: Add error types
pub fn Loop(comptime options: Options) type {
    return struct {
        const Self = @This();
        /// io_uring instance
        ring: IO_Uring,
        /// count of CQEs that we're waiting to receive, includes;
        /// * I/O operations that're submitted successfully (not in unqueued),
        /// * I/O operations that create multiple CQEs (multishot operations, zero-copy send etc.).
        io_pending: u32 = 0,
        /// I/O operations that're not queued yet
        unqueued: Intrusive(Completion) = .{},
        /// Completions that're ready for their callbacks to run
        completed: Intrusive(Completion) = .{},

        /// TODO: Check io_uring capabilities of kernel.
        /// Initializes a new event loop backed by io_uring.
        pub fn init() !Self {
            var flags: u32 = 0;

            // TODO: Make these optional.
            flags |= linux.IORING_SETUP_SINGLE_ISSUER;
            flags |= linux.IORING_SETUP_DEFER_TASKRUN;

            flags |= linux.IORING_SETUP_COOP_TASKRUN;
            flags |= linux.IORING_SETUP_TASKRUN_FLAG;

            return .{ .ring = try IO_Uring.init(256, flags) };
        }

        /// Deinitializes the event loop.
        pub fn deinit(self: *Self) void {
            self.ring.deinit();
        }

        /// NOTE: experimental
        pub inline fn hasIo(self: *const Self) bool {
            return self.io_pending > 0 and self.unqueued.isEmpty();
        }

        /// Runs the event loop until all operations are completed.
        pub fn run(self: *Self) !void {
            var cqes: [512]io_uring_cqe = undefined;

            while (self.hasIo()) {
                // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
                try self.flushSubmissions(&cqes);
                // We can now just peek for any CQEs without waiting and without another syscall:
                try self.flushCompletions(&cqes, self.io_pending);

                // Retry queueing completions that were unable to be queued before
                {
                    var copy = self.unqueued;
                    self.unqueued = .{};
                    while (copy.pop()) |c| self.enqueue(c);
                }

                // Run completions
                while (self.completed.pop()) |c| {
                    c.callback(self, c);
                }
            }
        }

        fn flushCompletions(self: *Self, slice: []io_uring_cqe, wait_nr: u32) !void {
            var remaining = wait_nr;

            while (true) {
                const completed = self.ring.copy_cqes(slice, remaining) catch |err| switch (err) {
                    error.SignalInterrupt => continue,
                    else => return err,
                };

                if (completed > remaining) remaining = 0 else remaining -= completed;

                // Decrement as much as completed events.
                // Currently, `io_pending` is only decremented here.
                self.io_pending -= completed;

                for (slice[0..completed]) |*cqe| {
                    const c: *Completion = @ptrFromInt(cqe.user_data);
                    // Set the result and CQE flags for the completion.
                    c.result = cqe.res;
                    c.flags = cqe.flags;
                    // We do not run the completion here (instead appending to a linked list) to avoid:
                    // * recursion through `flush_submissions()` and `flush_completions()`,
                    // * unbounded stack usage, and
                    // * confusing stack traces.
                    self.completed.push(c);
                }

                if (completed < slice.len) break;
            }
        }

        fn flushSubmissions(self: *Self, slice: []io_uring_cqe) !void {
            while (true) {
                _ = self.ring.submit_and_wait(1) catch |err| switch (err) {
                    error.SignalInterrupt => continue,
                    error.CompletionQueueOvercommitted, error.SystemResources => {
                        log.info("completion queue is overcommitted, flushing early", .{});
                        try self.flushCompletions(slice, 1);
                        continue;
                    },
                    else => return err,
                };

                break;
            }
        }

        /// Registers a completion to the loop.
        fn enqueue(self: *Self, c: *Completion) void {
            // get a submission queue entry
            const sqe = self.ring.get_sqe() catch |err| switch (err) {
                //if SQE ring is full, the completion will be added to unqueued.
                error.SubmissionQueueFull => return self.unqueued.push(c),
            };

            // prepare the completion.
            switch (c.operation) {
                .none => {
                    log.err("received a completion with `none` operation", .{});
                    unreachable;
                },
                .read => |op| {
                    sqe.prep_rw(.READ, op.handle, @intFromPtr(op.buffer.ptr), op.buffer.len, op.offset);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .write => |op| {
                    sqe.prep_rw(.WRITE, op.handle, @intFromPtr(op.buffer.ptr), op.buffer.len, op.offset);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .connect => |*op| {
                    sqe.prep_connect(op.socket, &op.addr.any, op.addr.getOsSockLen());

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .accept => |*op| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => {
                        // NOTE: Direct descriptors don't support SOCK_CLOEXEC flag at the moment.
                        // https://github.com/axboe/liburing/issues/1330
                        sqe.prep_multishot_accept_direct(op.socket, &op.addr, &op.addr_size, 0);
                        // op.socket is also a direct descriptor
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    },
                    // regular multishot accept
                    false => sqe.prep_multishot_accept(op.socket, &op.addr, &op.addr_size, posix.SOCK.CLOEXEC),
                },
                .recv => |op| {
                    sqe.prep_rw(.RECV, op.socket, @intFromPtr(op.buffer.ptr), op.buffer.len, 0);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .send => |op| {
                    sqe.prep_rw(.SEND, op.socket, @intFromPtr(op.buffer.ptr), op.buffer.len, 0);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                // FIXME: IORING_TIMEOUT_ETIME_SUCCESS seems to have no effect here
                .timeout => |*ts| sqe.prep_timeout(ts, 0, linux.IORING_TIMEOUT_ETIME_SUCCESS),
                .fds_update => |op| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => ex.io_uring_prep_files_update(sqe, op.fds, op.offset),
                    false => unreachable,
                },
                .close => |fd| switch (comptime options.io_uring.direct_descriptors_mode) {
                    true => sqe.prep_close_direct(@intCast(fd)), // works similar to fds_update
                    false => sqe.prep_close(fd),
                },
            }

            // userdata of an SQE is always set to a completion object.
            sqe.user_data = @intFromPtr(c);

            // we got a pending io
            self.io_pending += 1;
        }

        pub const ReadError = error{
            EndOfStream,
            NotOpenForReading,
            ConnectionResetByPeer,
            Alignment,
            InputOutput,
            IsDir,
            SystemResources,
            Unseekable,
            ConnectionTimedOut,
        } || CancellationError || UnexpectedError;

        /// Queues a read operation.
        pub fn read(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle: Handle,
            buffer: []u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                buffer: []u8,
                result: ReadError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .read = .{
                        .handle = handle,
                        .buffer = buffer,
                        // offset is a u64 but if the value is -1 then it uses the offset in the fd.
                        .offset = @bitCast(@as(i64, -1)),
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: ReadError!usize = if (res <= 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .SUCCESS => error.EndOfStream,
                            .INTR, .AGAIN => {
                                // Some file systems, like XFS, can return EAGAIN even when
                                // reading from a blocking file without flags like RWF_NOWAIT.
                                return loop.enqueue(c);
                            },
                            .BADF => error.NotOpenForReading,
                            .CONNRESET => error.ConnectionResetByPeer,
                            .FAULT => unreachable,
                            .INVAL => error.Alignment,
                            .IO => error.InputOutput,
                            .ISDIR => error.IsDir,
                            .CANCELED => error.Cancelled,
                            .NOBUFS => error.SystemResources,
                            .NOMEM => error.SystemResources,
                            .NXIO => error.Unseekable,
                            .OVERFLOW => error.Unseekable,
                            .SPIPE => error.Unseekable,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            else => |err| posix.unexpectedErrno(err),
                        } else @intCast(res);

                        const buf = c.operation.read.buffer;
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, buf, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const WriteError = error{
            WouldBlock,
            NotOpenForWriting,
            NotConnected,
            DiskQuota,
            FileTooBig,
            Alignment,
            InputOutput,
            NoSpaceLeft,
            Unseekable,
            AccessDenied,
            BrokenPipe,
        } || CancellationError || UnexpectedError;

        /// Queues a write operation.
        pub fn write(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle: Handle,
            buffer: []const u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                handle: Handle,
                buffer: []const u8,
                result: WriteError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .write = .{
                        .handle = handle,
                        .buffer = buffer,
                        // offset is a u64 but if the value is -1 then it uses the offset in the fd.
                        .offset = comptime @bitCast(@as(i64, -1)),
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.returnType(.write) = if (res <= 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .SUCCESS => error.EndOfStream, // 0 read
                            .INTR => return loop.enqueue(completion),
                            .AGAIN => error.WouldBlock,
                            .BADF => error.NotOpenForWriting,
                            .CANCELED => error.Cancelled,
                            .DESTADDRREQ => error.NotConnected,
                            .DQUOT => error.DiskQuota,
                            .FAULT => unreachable,
                            .FBIG => error.FileTooBig,
                            .INVAL => error.Alignment,
                            .IO => error.InputOutput,
                            .NOSPC => error.NoSpaceLeft,
                            .NXIO => error.Unseekable,
                            .OVERFLOW => error.Unseekable,
                            .PERM => error.AccessDenied,
                            .PIPE => error.BrokenPipe,
                            .SPIPE => error.Unseekable,
                            else => error.Unexpected,
                        } else @intCast(res);

                        const op = c.operation.write;
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.handle, op.buffer, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const ConnectError = error{
            AccessDenied,
            AddressInUse,
            AddressNotAvailable,
            AddressFamilyNotSupported,
            WouldBlock,
            OpenAlreadyInProgress,
            FileDescriptorInvalid,
            ConnectionRefused,
            ConnectionResetByPeer,
            AlreadyConnected,
            NetworkUnreachable,
            HostUnreachable,
            FileNotFound,
            FileDescriptorNotASocket,
            PermissionDenied,
            ProtocolNotSupported,
            ConnectionTimedOut,
            SystemResources,
        } || CancellationError || UnexpectedError;

        /// Queues a connect operation.
        pub fn connect(
            self: *Self,
            comptime T: type,
            userdata: *T,
            completion: *Completion,
            s: Socket,
            addr: std.net.Address,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                socket: Socket,
                addr: std.net.Address,
                result: ConnectError!void,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .connect = .{
                        .socket = s,
                        .addr = addr,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.returnType(.connect) = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .INTR => return loop.enqueue(c),
                            .ACCES => error.AccessDenied,
                            .ADDRINUSE => error.AddressInUse,
                            .ADDRNOTAVAIL => error.AddressNotAvailable,
                            .AFNOSUPPORT => error.AddressFamilyNotSupported,
                            .AGAIN, .INPROGRESS => error.WouldBlock,
                            .ALREADY => error.OpenAlreadyInProgress,
                            .BADF => error.FileDescriptorInvalid,
                            .CANCELED => error.Cancelled,
                            .CONNREFUSED => error.ConnectionRefused,
                            .CONNRESET => error.ConnectionResetByPeer,
                            .FAULT => unreachable,
                            .ISCONN => error.AlreadyConnected,
                            .NETUNREACH => error.NetworkUnreachable,
                            .HOSTUNREACH => error.HostUnreachable,
                            .NOENT => error.FileNotFound,
                            .NOTSOCK => error.FileDescriptorNotASocket,
                            .PERM => error.PermissionDenied,
                            .PROTOTYPE => error.ProtocolNotSupported,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            else => error.Unexpected,
                        };

                        const op = c.operation.connect;
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.addr, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // error that's not really expected
        pub const UnexpectedError = error{Unexpected};
        // error that's used on operations that can be cancelled
        pub const CancellationError = error{Cancelled};

        // possible errors accept can create
        pub const AcceptError = error{
            WouldBlock,
            ConnectionAborted,
            SocketNotListening,
            ProcessFdQuotaExceeded,
            SystemFdQuotaExceeded,
            SystemResources,
            OperationNotSupported,
            PermissionDenied,
            ProtocolFailure,
        } || CancellationError || UnexpectedError;

        /// Starts accepting connections for a given socket.
        /// This a multishot operation, meaning it'll keep on running until it's either cancelled or encountered with an error.
        /// Completions given to multishot operations MUST NOT be reused until the multishot operation is either cancelled or encountered with an error.
        pub fn accept(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                socket: Socket,
                result: AcceptError!Socket,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .accept = .{
                        .socket = s,
                        .addr = undefined,
                        .addr_size = @sizeOf(posix.sockaddr),
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.returnType(.accept) = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .INTR => return loop.enqueue(c), // we're interrupted, try again
                            .AGAIN => error.WouldBlock,
                            .BADF => error.Unexpected,
                            .CONNABORTED => error.ConnectionAborted,
                            .FAULT => error.Unexpected,
                            .INVAL => error.SocketNotListening,
                            .MFILE => error.ProcessFdQuotaExceeded,
                            .NFILE => error.SystemFdQuotaExceeded,
                            .NOBUFS => error.SystemResources,
                            .NOMEM => error.SystemResources,
                            .NOTSOCK => error.Unexpected,
                            .OPNOTSUPP => error.OperationNotSupported,
                            .PERM => error.PermissionDenied,
                            .PROTO => error.ProtocolFailure,
                            .CANCELED => error.Cancelled,
                            else => error.Unexpected,
                        } else res; // valid socket

                        // we're accepting via `io_uring_prep_multishot_accept` so we have to increment pending for each successful accept request
                        // to keep the loop running.
                        // this must be done no matter what `cqe.res` is given.
                        if (c.flags & linux.IORING_CQE_F_MORE != 0) {
                            loop.io_pending += 1;
                        }

                        const listener = c.operation.accept.socket;
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, listener, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const RecvError = error{
            EndOfStream,
            WouldBlock,
            SystemResources,
            OperationNotSupported,
            PermissionDenied,
            SocketNotConnected,
            ConnectionResetByPeer,
            ConnectionTimedOut,
            ConnectionRefused,
        } || CancellationError || UnexpectedError;

        /// Queues a recv operation.
        pub fn recv(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            buffer: []u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                socket: Socket,
                buffer: []u8,
                result: RecvError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .recv = .{
                        .socket = s,
                        .buffer = buffer,
                    },
                },
                .userdata = userdata,
                .callback = struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: RecvError!usize = if (res <= 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .SUCCESS => error.EndOfStream, // 0 reads are interpreted as errors
                            .INTR => return loop.enqueue(c), // we're interrupted, try again
                            .AGAIN => error.WouldBlock,
                            .BADF => error.Unexpected,
                            .CONNREFUSED => error.ConnectionRefused,
                            .FAULT => unreachable,
                            .INVAL => unreachable,
                            .NOBUFS => error.SystemResources,
                            .NOMEM => error.SystemResources,
                            .NOTCONN => error.SocketNotConnected,
                            .NOTSOCK => error.Unexpected,
                            .CONNRESET => error.ConnectionResetByPeer,
                            .TIMEDOUT => error.ConnectionTimedOut,
                            .OPNOTSUPP => error.OperationNotSupported,
                            .CANCELED => error.Cancelled,
                            else => error.Unexpected,
                        } else @intCast(res); // valid

                        const op = c.operation.recv;
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        //pub const RecvPooledError = RecvError || error{OutOfBuffers};

        /// NOTE: Experimental.
        ///
        /// Queues a recv operation, this variant uses a provided buffer pool instead of a single buffer.
        ///
        /// io_uring supports a mechanism called provided buffers. The application sets
        /// aside a pool of buffers, and informs io_uring of those buffers. This allows the kernel to
        /// pick a suitable buffer when the given receive operation is ready to actually receive data,
        /// rather than upfront. The CQE posted will then additionally hold information about
        /// which buffer was picked.
        /// https://kernel.dk/io_uring%20and%20networking%20in%202023.pdf
        //pub fn recvPooled(
        //    self: *Self,
        //    completion: *Completion,
        //    comptime T: type,
        //    userdata: *T,
        //    s: Socket,
        //    buffer_pool: *BufferPool,
        //    comptime callback: *const fn (
        //        userdata: *T,
        //        loop: *Self,
        //        completion: *Completion,
        //        socket: Socket,
        //        buffer_pool: *BufferPool,
        //        buffer_id: u16,
        //        result: RecvPooledError!usize,
        //    ) void,
        //) void {
        //    completion.* = .{
        //        .next = null,
        //        .operation = .{
        //            .recv_bp = .{
        //                .socket = s,
        //                .buf_pool = buffer_pool,
        //            },
        //        },
        //        .userdata = userdata,
        //        .callback = struct {
        //            fn wrap(loop: *Self, c: *Completion) void {
        //                const res = c.result;
        //                const flags = c.flags;
        //                const op = c.operation.recv_bp;
        //
        //                const result: Completion.OperationType.returnType(.recv_bp) = if (res <= 0)
        //                    switch (@as(posix.E, @enumFromInt(-res))) {
        //                        .SUCCESS => error.EndOfStream, // 0 reads are interpreted as errors
        //                        .INTR => return loop.enqueue(c), // we're interrupted, try again
        //                        .AGAIN => error.WouldBlock,
        //                        .BADF => error.Unexpected,
        //                        .CONNREFUSED => error.ConnectionRefused,
        //                        .FAULT => unreachable,
        //                        .INVAL => unreachable,
        //                        .NOBUFS => error.OutOfBuffers,
        //                        .NOMEM => error.SystemResources,
        //                        .NOTCONN => error.SocketNotConnected,
        //                        .NOTSOCK => error.Unexpected,
        //                        .CONNRESET => error.ConnectionResetByPeer,
        //                        .TIMEDOUT => error.ConnectionTimedOut,
        //                        .OPNOTSUPP => error.OperationNotSupported,
        //                        .CANCELED => error.Cancelled,
        //                        else => error.Unexpected,
        //                    }
        //                else
        //                    @intCast(res);
        //
        //                // which buffer did the worker pick from the pool?
        //                const buffer_id: u16 = @intCast(flags >> linux.IORING_CQE_BUFFER_SHIFT);
        //
        //                // The application should check the flags of each CQE,
        //                // regardless of its result. If a posted CQE does not have the
        //                // IORING_CQE_F_MORE flag set then the multishot receive will be
        //                // done and the application should issue a new request.
        //                // https://man7.org/linux/man-pages/man3/io_uring_prep_recv_multishot.3.html
        //                if (flags & linux.IORING_CQE_F_MORE != 0) {
        //                    loop.io_pending += 1;
        //                }
        //
        //                // NOTE: Currently its up to caller to give the buffer back to kernel.
        //                // This behaviour might change in the future though.
        //                @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buf_pool, buffer_id, result });
        //            }
        //        }.wrap,
        //    };
        //
        //    self.enqueue(completion);
        //}

        pub const SendError = error{
            AccessDenied,
            WouldBlock,
            FastOpenAlreadyInProgress,
            AddressFamilyNotSupported,
            FileDescriptorInvalid,
            ConnectionResetByPeer,
            MessageTooBig,
            SystemResources,
            SocketNotConnected,
            FileDescriptorNotASocket,
            OperationNotSupported,
            BrokenPipe,
            ConnectionTimedOut,
        } || CancellationError || UnexpectedError;

        /// Queues a send operation.
        pub fn send(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            buffer: []const u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                socket: Socket,
                buffer: []const u8,
                result: SendError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .send = .{
                        .socket = s,
                        .buffer = buffer,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: SendError!usize = blk: {
                            if (res < 0) {
                                const err = switch (@as(posix.E, @enumFromInt(-res))) {
                                    .INTR => {
                                        loop.enqueue(completion);
                                        return;
                                    },
                                    .ACCES => error.AccessDenied,
                                    .AGAIN => error.WouldBlock,
                                    .ALREADY => error.FastOpenAlreadyInProgress,
                                    .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                    .BADF => error.FileDescriptorInvalid,
                                    // Can happen when send()'ing to a UDP socket.
                                    .CONNREFUSED => error.ConnectionRefused,
                                    .CONNRESET => error.ConnectionResetByPeer,
                                    .DESTADDRREQ => unreachable,
                                    .FAULT => unreachable,
                                    .INVAL => unreachable,
                                    .ISCONN => unreachable,
                                    .MSGSIZE => error.MessageTooBig,
                                    .NOBUFS => error.SystemResources,
                                    .NOMEM => error.SystemResources,
                                    .NOTCONN => error.SocketNotConnected,
                                    .NOTSOCK => error.FileDescriptorNotASocket,
                                    .OPNOTSUPP => error.OperationNotSupported,
                                    .PIPE => error.BrokenPipe,
                                    .TIMEDOUT => error.ConnectionTimedOut,
                                    else => |errno| posix.unexpectedErrno(errno),
                                };
                                break :blk err;
                            } else {
                                break :blk @intCast(res);
                            }
                        };

                        // regular send operations
                        const op = c.operation.send;
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.socket, op.buffer, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const TimeoutError = CancellationError || UnexpectedError;

        /// TODO: we might want to prefer IORING_TIMEOUT_ABS here.
        /// Queues a timeout operation.
        /// nanoseconds are given as u63 for coercion to i64.
        pub fn timeout(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            ns: u63,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                result: TimeoutError!void,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{
                    .timeout = .{ .sec = 0, .nsec = ns },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        const result: TimeoutError!void = if (res < 0) switch (@as(posix.E, @enumFromInt(-res))) {
                            .TIME => {}, // still a success
                            .INVAL => unreachable,
                            .FAULT => unreachable,
                            .CANCELED => error.Cancelled,
                            else => |err| posix.unexpectedErrno(err),
                        } else {};

                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        pub const HandleOrSocket = linux.fd_t;

        /// Queues a close operation.
        /// Supports both file descriptors and direct descriptors.
        pub fn close(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle_or_socket: HandleOrSocket,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{ .close = handle_or_socket },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        const res = c.result;

                        // According to Zen, close operations should not fail.
                        // So we do not return any errors here.
                        switch (@as(posix.E, @enumFromInt(-res))) {
                            .BADF => unreachable, // Always a race condition.
                            .INTR => {}, // This is still a success. See https://github.com/ziglang/zig/issues/2425
                            else => {},
                        }

                        // execute the user provided callback
                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        /// Creates a new socket.
        pub inline fn socket(_: Self, domain: u32, socket_type: u32, protocol: u32) !Socket {
            return posix.socket(domain, socket_type, protocol);
        }

        /// Sets REUSEADDR flag on a given socket.
        pub inline fn setReuseAddr(_: Self, s: Socket) posix.SetSockOptError!void {
            return posix.setsockopt(s, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        }

        /// Sets REUSEPORT flag on a given socket if possible.
        pub inline fn setReusePort(_: Self, s: Socket) posix.SetSockOptError!void {
            if (@hasDecl(posix.SO, "REUSEPORT")) {
                return posix.setsockopt(s, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
            }
        }

        /// Enables/disables TCP_NODELAY setting on a given socket.
        pub inline fn setTcpNoDelay(_: Self, s: Socket, on: bool) posix.SetSockOptError!void {
            return posix.setsockopt(s, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
        }

        /// Enables/disables SO_KEEPALIVE setting on a given socket.
        pub inline fn setTcpKeepAlive(_: Self, s: Socket, on: bool) posix.SetSockOptError!void {
            return posix.setsockopt(s, posix.SOL.SOCKET, posix.SO.KEEPALIVE, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
        }

        pub const Registration = enum(u1) { regular, sparse };

        /// TODO: We might want to support resource tagging too, needs further investigations.
        ///
        /// io_uring only.
        ///
        /// Registers file descriptors that're going to be used within ring.
        /// * `regular` option requires a constant array of file descriptors.
        /// * `sparse` option allocates an invalid (-1) array of direct descriptors for the ring. Good if you want to register file descriptors later on.
        pub fn directDescriptors(
            self: *Self,
            comptime registration: Registration,
            array_or_u32: switch (registration) {
                .regular => []const linux.fd_t,
                .sparse => u32,
            },
        ) !void {
            if (comptime !options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            return switch (registration) {
                .regular => self.ring.register_files(array_or_u32),
                .sparse => ex.io_uring_register_files_sparse(&self.ring, array_or_u32),
            };
        }

        /// TODO: We might want to support resource tagging too, needs further investigations.
        /// io_uring only.
        ///
        /// Updates file descriptors. Starting from the `offset`.
        /// This function works synchronously, which might suit better for some situations.
        /// Check `updateDescriptorsAsync` function if you need asynchronous update.
        pub fn updateDescriptorsSync(self: *Self, offset: u32, fds: []const linux.fd_t) !void {
            if (comptime !options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            return self.ring.register_files_update(offset, fds);
        }

        pub const UpdateFdsError = error{
            OutOfMemory,
            InvalidArguments,
            UnableToCopyFileDescriptors,
            InvalidFileDescriptor,
            FileDescriptorOverflow,
        } || UnexpectedError;

        // Since this utility is used for both regular and alloc variants, it's better to have it write here once.
        fn getUpdateFdsResult(res: i32) UpdateFdsError!i32 {
            // FIXME: Is this operation cancellable?
            // FIXME: Are there any other errors this can produce?
            // https://www.man7.org/linux/man-pages/man3/io_uring_prep_files_update.3.html
            if (res < 0) {
                return switch (@as(posix.E, @enumFromInt(-res))) {
                    .NOMEM => error.OutOfMemory,
                    .INVAL => error.InvalidArguments,
                    .FAULT => error.UnableToCopyFileDescriptors,
                    .BADF => error.InvalidFileDescriptor,
                    .OVERFLOW => error.FileDescriptorOverflow,
                    else => |err| posix.unexpectedErrno(err),
                };
            }

            return res;
        }

        /// io_uring only.
        ///
        /// Queues an `io_uring_prep_files_update` operation, similar to `updateDescriptors` but happens in async manner.
        pub fn updateDescriptorsAsync(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            fds: []const linux.fd_t,
            offset: u32, // NOTE: type of this might be a problem but I'm not totally sure
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                fds: []const linux.fd_t,
                offset: u32,
                result: UpdateFdsError!i32, // result can only be max. int32 either
            ) void,
        ) void {
            if (comptime !options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            completion.* = .{
                .next = null,
                .operation = .{
                    .fds_update = .{
                        .fds = @constCast(fds),
                        .offset = offset,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        // get the result of this operation
                        const result = getUpdateFdsResult(c.result);
                        const op = c.operation.fds_update;

                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.fds, op.offset, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        /// io_uring only.
        ///
        /// Same as `updateDescriptorsAsync` but this variant doesn't take `offset` argument. Instead, it allocates fds
        /// to empty spots and fills the given `fds` slice with direct descriptors. Order is preserved.
        pub fn updateDescriptorsAsyncAlloc(
            self: *Self,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            fds: []linux.fd_t,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Self,
                completion: *Completion,
                fds: []linux.fd_t,
                result: UpdateFdsError!i32, // result can only be max. int32 either
            ) void,
        ) void {
            if (comptime !options.io_uring.direct_descriptors_mode) {
                @compileError("direct descriptors are not enabled");
            }

            completion.* = .{
                .next = null,
                .operation = .{
                    .fds_update = .{
                        .fds = fds,
                        .offset = linux.IORING_FILE_INDEX_ALLOC,
                    },
                },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Self, c: *Completion) void {
                        // get the result of this operation
                        const result = getUpdateFdsResult(c.result);
                        const op = c.operation.fds_update;

                        @call(.always_inline, callback, .{ @as(*T, @ptrCast(@alignCast(c.userdata))), loop, c, op.fds, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

        // callback used for an empty Completion.
        const emptyCallback = struct {
            fn callback(_: *Self, _: *Completion) void {}
        }.callback;

        // Represents operations.
        pub const Completion = struct {
            /// Intrusively linked
            next: ?*Completion = null,
            /// Userdata and callback for when the completion is finished
            userdata: ?*anyopaque = null,
            /// NOTE: IDK if null or undefined suit here better
            callback: Callback = emptyCallback,
            /// Result from the CQE, filled after the operation is completed.
            result: i32 = 0,
            /// This is used in two ways:
            /// * When a completion is submitted, additional flags (SQE, send flags etc.) can be set here.
            /// * When a completion is finished, CQE flags will be set here.
            flags: u32 = 0,
            /// Operation intended by this completion
            operation: Operation = .none,

            // callback to be invoked when an operation is complete.
            pub const Callback = *const fn (_: *Self, _: *Completion) void;

            // operation kind
            pub const OperationType = enum {
                none,
                read,
                write,
                connect,
                accept,
                recv,
                send,
                timeout,
                //cancel,
                fds_update,
                close,

                /// Returns the return type for the given operation type.
                /// Can be run on comptime, but not necessarily.
                pub fn returnType(op: OperationType) type {
                    return switch (op) {
                        .none => unreachable,
                        .read => ReadError!usize,
                        .write => WriteError!usize,
                        .connect => ConnectError!void,
                        .accept => AcceptError!Socket,
                        .recv => RecvError!usize,
                        .send => SendError!usize,
                        .timeout => TimeoutError!void,
                        .fds_update => UpdateFdsError!i32,
                        .close => void,
                    };
                }
            };

            // values needed by operations
            // TODO: hide the io_uring specific operation kinds behind a comptime switch, like `fds_update`.
            pub const Operation = union(OperationType) {
                // fds_update comptime magic
                pub const FdsUpdate = switch (options.io_uring.direct_descriptors_mode) {
                    true => struct {
                        fds: []linux.fd_t,
                        offset: u32,
                    },
                    false => void,
                };

                // various kinds of cancel operations are supported
                // https://man7.org/linux/man-pages/man3/io_uring_prep_cancel.3.html
                //pub const CancelType = enum(u1) { all_io, completion };

                //pub const Cancel = union(CancelType) {
                //    all_io: Handle,
                //    completion: *Completion,
                //};

                // default
                none: void,
                read: struct {
                    handle: linux.fd_t,
                    buffer: []u8,
                    offset: u64,
                },
                write: struct {
                    handle: linux.fd_t,
                    buffer: []const u8,
                    offset: u64,
                },
                connect: struct {
                    socket: linux.fd_t,
                    addr: std.net.Address,
                },
                // TODO: take `*posix.sockaddr` instead
                accept: struct {
                    socket: linux.fd_t, // listener socket
                    addr: posix.sockaddr = undefined,
                    addr_size: posix.socklen_t = @sizeOf(posix.sockaddr),
                },
                recv: struct {
                    socket: Socket,
                    buffer: []u8,
                },
                send: struct {
                    socket: linux.fd_t,
                    buffer: []const u8,
                },
                timeout: linux.kernel_timespec,
                fds_update: FdsUpdate,
                close: HandleOrSocket,
            };
        };

        // on unix, there's a unified fd type that's used for both sockets and file, pipe etc. handles.
        // since this library is intended for cross platform usage and windows separates file descriptors by different kinds, I've decided to adapt the same mindset to here.
        pub const Handle = linux.fd_t;
        pub const Socket = linux.fd_t;
    };
}

test "io_uring: direct descriptors" {
    const DefaultLoop = Loop(.{
        .io_uring = .{
            .direct_descriptors_mode = true,
        },
    });

    const Completion = DefaultLoop.Completion;

    // create a loop
    var loop = try DefaultLoop.init();
    defer loop.deinit();

    // get file descriptor limits
    const rlimit = try posix.getrlimit(.NOFILE);

    // create direct descriptors table
    try loop.directDescriptors(.sparse, @intCast(rlimit.max & std.math.maxInt(u32)));

    const server = try loop.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
    try loop.setReuseAddr(server);
    try loop.setTcpNoDelay(server, true);

    try loop.updateDescriptorsSync(0, &[_]linux.fd_t{server});

    var close_c = Completion{};

    loop.close(&close_c, Completion, &close_c, 0, struct {
        fn onClose(_: *Completion, _: *DefaultLoop, _: *Completion) void {}
    }.onClose);

    try loop.run();
}
