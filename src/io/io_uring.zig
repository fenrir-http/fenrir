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
const ex = @import("linux/io_uring_ex.zig");

const log = std.log.scoped(.loop);

/// Event notifier implementation based on io_uring.
pub fn LoopImpl(comptime options: Options) type {
    return struct {
        const Loop = @This();
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
        pub fn init() !Loop {
            var flags: u32 = 0;

            // TODO: Make these optional.
            flags |= linux.IORING_SETUP_SINGLE_ISSUER;
            flags |= linux.IORING_SETUP_DEFER_TASKRUN;

            flags |= linux.IORING_SETUP_COOP_TASKRUN;
            flags |= linux.IORING_SETUP_TASKRUN_FLAG;

            return .{ .ring = try IO_Uring.init(256, flags) };
        }

        /// Deinitializes the event loop.
        pub fn deinit(self: *Loop) void {
            self.ring.deinit();
        }

        /// NOTE: experimental
        pub inline fn hasIo(self: *const Loop) bool {
            return self.io_pending > 0 and self.unqueued.isEmpty();
        }

        /// Runs the event loop until all operations are completed.
        pub fn run(self: *Loop) !void {
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
                    c.perform(self);
                }
            }
        }

        fn flushCompletions(self: *Loop, slice: []io_uring_cqe, wait_nr: u32) !void {
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

        fn flushSubmissions(self: *Loop, slice: []io_uring_cqe) !void {
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
        fn enqueue(self: *Loop, c: *Completion) void {
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
                .read => {
                    const metadata = c.getMetadata(.read, false);
                    sqe.prep_rw(.READ, metadata.handle, @intFromPtr(metadata.base), metadata.len, @bitCast(@as(i64, -1)));

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .write => {
                    const metadata = c.getMetadata(.write, false);
                    sqe.prep_rw(.WRITE, metadata.handle, @intFromPtr(metadata.base), metadata.len, @bitCast(@as(i64, -1)));

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .writev => {
                    const metadata = c.getMetadata(.writev, false);
                    sqe.prep_writev(metadata.handle, @ptrCast(metadata.iovecs[0..metadata.nr_vecs]), comptime @bitCast(@as(i64, -1)));

                    // We want to use direct descriptors.
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .connect => {
                    const metadata = c.getMetadata(.connect, true);
                    sqe.prep_connect(metadata.socket, &metadata.addr, metadata.socklen);

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .accept => {
                    const metadata = c.getMetadata(.accept, true);

                    switch (comptime options.io_uring.direct_descriptors_mode) {
                        true => {
                            // NOTE: Direct descriptors don't support SOCK_CLOEXEC flag at the moment.
                            // https://github.com/axboe/liburing/issues/1330
                            sqe.prep_multishot_accept_direct(metadata.socket, &metadata.addr, &metadata.socklen, 0);
                            // op.socket is also a direct descriptor
                            sqe.flags |= linux.IOSQE_FIXED_FILE;
                        },
                        // regular multishot accept
                        false => sqe.prep_multishot_accept(metadata.socket, &metadata.addr, &metadata.socklen, posix.SOCK.CLOEXEC),
                    }
                },
                .recv => {
                    const metadata = c.getMetadata(.recv, false);
                    sqe.prep_rw(.RECV, metadata.socket, @intFromPtr(metadata.base), metadata.len, 0);
                    // recv flags.
                    sqe.rw_flags = metadata.flags;

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                .send => {
                    const metadata = c.getMetadata(.send, false);
                    sqe.prep_rw(.SEND, metadata.socket, @intFromPtr(metadata.base), metadata.len, 0);
                    // send flags.
                    sqe.rw_flags = metadata.flags;

                    // we want to use direct descriptors
                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.flags |= linux.IOSQE_FIXED_FILE;
                    }
                },
                // FIXME: IORING_TIMEOUT_ETIME_SUCCESS seems to have no effect here
                .timeout => {
                    const metadata = c.getMetadata(.timeout, true);
                    sqe.prep_timeout(&metadata.timespec, 0, linux.IORING_TIMEOUT_ETIME_SUCCESS);
                },
                .close => {
                    const metadata = c.getMetadata(.close, false);

                    if (comptime options.io_uring.direct_descriptors_mode) {
                        sqe.prep_close_direct(@intCast(metadata.handle));
                    } else {
                        sqe.prep_close(metadata.handle);
                    }
                },
            }

            // userdata of an SQE is always set to a completion object.
            sqe.user_data = @intFromPtr(c);

            // we got a pending io
            self.io_pending += 1;
        }

        pub const ReadError = error{
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle: Handle,
            buffer: []u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
                buffer: []u8,
                result: ReadError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .read,
                .metadata = .{
                    .read = .{
                        .base = buffer.ptr,
                        .len = buffer.len,
                        .handle = handle,
                    },
                },
                .userdata = userdata,
                // We erase the type of function pointer here since we're using extern structs.
                // Extern structs can only represent `*const anyopaque` for functions at most.
                .callback = @ptrCast(&(comptime struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.Returns(.read) = if (res < 0) switch (c.err()) {
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

                        const metadata = c.getMetadata(.read, false);
                        const buf = metadata.base[0..metadata.len];

                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ c.castUserdata(T), loop, c, buf, result });
                    }
                }.wrap)),
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle: Handle,
            buffer: []const u8,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
                buffer: []const u8,
                result: WriteError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .write,
                .metadata = .{
                    .write = .{
                        .base = buffer.ptr,
                        .len = buffer.len,
                        .handle = handle,
                    },
                },
                .userdata = userdata,
                // following is what happens when we receive a sqe for this completion.
                .callback = @ptrCast(&(comptime struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.Returns(.write) = if (res < 0) switch (c.err()) {
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
                            else => |err| posix.unexpectedErrno(err),
                        } else @intCast(res);

                        const metadata = c.getMetadata(.write, false);
                        const buf = metadata.base[0..metadata.len];
                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ c.castUserdata(T), loop, c, buf, result });
                    }
                }.wrap)),
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            addr: std.net.Address,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
                result: ConnectError!void,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .connect,
                .metadata = .{
                    .connect = .{
                        .addr = addr.any,
                        .socket = s,
                        .socklen = addr.getOsSockLen(),
                    },
                },
                .userdata = userdata,
                .callback = @ptrCast(&(comptime struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.Returns(.connect) = if (res < 0) switch (c.err()) {
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
                            else => |err| posix.unexpectedErrno(err),
                        };

                        @call(.always_inline, callback, .{ c.castUserdata(T), loop, c, result });
                    }
                }.wrap)),
            };

            self.enqueue(completion);
        }

        // Error that's not really expected.
        pub const UnexpectedError = error{Unexpected};
        // Error that's expected on operations that're cancelable.
        pub const CancellationError = error{Cancelled};

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

        /// Start accepting connections for a given socket.
        /// This a multishot operation, meaning it'll keep on running until it's either canceled or encountered with an error.
        /// Completions given to multishot operations MUST NOT be reused until the multishot operation is either canceled or encountered with an error.
        pub fn accept(
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
                result: AcceptError!Socket,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .accept,
                .metadata = .{
                    .accept = .{ .socket = s },
                },
                .userdata = userdata,
                .callback = @ptrCast(&(comptime struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
                        const res = c.result;

                        const result: Completion.OperationType.Returns(.accept) = if (res < 0) switch (c.err()) {
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
                            else => |err| posix.unexpectedErrno(err),
                        } else res; // valid socket

                        // We're accepting via `io_uring_prep_multishot_accept`
                        // so we have to increment pending for each successful accept request to keep the loop running.
                        // this must be done no matter what `cqe.res` is given.
                        if (c.flags & linux.IORING_CQE_F_MORE != 0) {
                            loop.io_pending += 1;
                        }

                        // invoke the user provided callback
                        @call(.always_inline, callback, .{ c.castUserdata(T), loop, c, result });
                    }
                }.wrap)),
            };

            self.enqueue(completion);
        }

        pub const RecvError = error{
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            buffer: []u8,
            flags: u32,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
                buffer: []u8,
                result: RecvError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .recv,
                .metadata = .{
                    .recv = .{
                        .base = buffer.ptr,
                        .len = buffer.len,
                        .socket = s,
                        .flags = flags,
                    },
                },
                .userdata = userdata,
                .callback = struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
                        const res = c.result;

                        const result: RecvError!usize = if (res < 0) switch (c.err()) {
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
                            else => |err| posix.unexpectedErrno(err),
                        } else @intCast(res); // valid

                        const metadata = c.getMetadata(.recv, false);
                        const buf = metadata[0..metadata.len];

                        @call(.always_inline, callback, .{ c.castUserdata(T), loop, c, buf, result });
                    }
                }.wrap,
            };

            self.enqueue(completion);
        }

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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            s: Socket,
            buffer: []const u8,
            flags: u32,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
                buffer: []const u8,
                result: SendError!usize,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .send,
                .metadata = .{
                    .send = .{
                        .base = buffer.ptr,
                        .len = buffer.len,
                        .socket = s,
                        .flags = flags,
                    },
                },
                .userdata = userdata,
                .callback = @ptrCast(&(comptime struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
                        const res = c.result;

                        const result: SendError!usize = blk: {
                            if (res < 0) {
                                const err = switch (c.err()) {
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

                        const metadata = c.getMetadata(.send, false);
                        const buf = metadata.base[0..metadata.len];

                        @call(.always_inline, callback, .{ c.castUserdata(T), loop, c, buf, result });
                    }
                }.wrap)),
            };

            self.enqueue(completion);
        }

        pub const TimeoutError = CancellationError || UnexpectedError;

        /// TODO: we might want to prefer IORING_TIMEOUT_ABS here.
        /// Queues a timeout operation.
        /// nanoseconds are given as u63 for coercion to i64.
        pub fn timeout(
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            ns: u63,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
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
                    fn wrap(loop: *Loop, c: *Completion) void {
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            handle_or_socket: HandleOrSocket,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
                completion: *Completion,
            ) void,
        ) void {
            completion.* = .{
                .next = null,
                .operation = .{ .close = handle_or_socket },
                .userdata = userdata,
                .callback = comptime struct {
                    fn wrap(loop: *Loop, c: *Completion) void {
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
        pub inline fn socket(_: Loop, domain: u32, socket_type: u32, protocol: u32) !Socket {
            return posix.socket(domain, socket_type, protocol);
        }

        /// Sets REUSEADDR flag on a given socket.
        pub inline fn setReuseAddr(_: Loop, s: Socket) posix.SetSockOptError!void {
            return posix.setsockopt(s, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        }

        /// Sets REUSEPORT flag on a given socket if possible.
        pub inline fn setReusePort(_: Loop, s: Socket) posix.SetSockOptError!void {
            if (@hasDecl(posix.SO, "REUSEPORT")) {
                return posix.setsockopt(s, posix.SOL.SOCKET, posix.SO.REUSEPORT, &std.mem.toBytes(@as(c_int, 1)));
            }
        }

        /// Enables/disables TCP_NODELAY setting on a given socket.
        pub inline fn setTcpNoDelay(_: Loop, s: Socket, on: bool) posix.SetSockOptError!void {
            return posix.setsockopt(s, posix.IPPROTO.TCP, posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, @intFromBool(on))));
        }

        /// Enables/disables SO_KEEPALIVE setting on a given socket.
        pub inline fn setTcpKeepAlive(_: Loop, s: Socket, on: bool) posix.SetSockOptError!void {
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
            self: *Loop,
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
        pub fn updateDescriptorsSync(self: *Loop, offset: u32, fds: []const linux.fd_t) !void {
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            fds: []const linux.fd_t,
            offset: u32, // NOTE: type of this might be a problem but I'm not totally sure
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
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
                    fn wrap(loop: *Loop, c: *Completion) void {
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
            self: *Loop,
            completion: *Completion,
            comptime T: type,
            userdata: *T,
            fds: []linux.fd_t,
            comptime callback: *const fn (
                userdata: *T,
                loop: *Loop,
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
                    fn wrap(loop: *Loop, c: *Completion) void {
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
            fn callback(_: *Loop, _: *Completion) void {}
        }.callback;

        // Represents operations.
        pub const Completion = extern struct {
            /// Intrusively linked
            next: ?*Completion = null,
            /// Userdata and callback for when the completion is finished
            userdata: ?*anyopaque = null,
            callback: *const anyopaque = @ptrCast(&emptyCallback),
            /// Result from the CQE, filled after the operation is completed.
            result: i32 = 0,
            /// This is used in two ways:
            /// * When a completion is submitted, additional flags (SQE, send flags etc.) can be set here (currently not preferred).
            /// * When a completion is finished, CQE flags will be set here.
            flags: u32 = 0,
            /// Operation intended by this completion
            operation: OperationType = .none, // 1
            /// Whether or not the completion is in the works.
            /// TODO: Not implemented yet.
            status: enum(u8) { idle, running } = .idle, // 1
            /// This'll be given by compiler anyway, reserved here for future usage.
            __pad: [6]u8 = undefined,
            /// Various operation metadata.
            metadata: Operation = .{ .none = {} }, // 24

            /// Type of callback that's invoked when an operation is complete.
            pub const Callback = *const fn (_: *Loop, _: *Completion) void;

            /// Adds type to `callback` since it's type-erased.
            pub inline fn CallbackFn(completion: *const Completion) Callback {
                return @ptrCast(completion.callback);
            }

            /// Executes the `callback` attached to this completion.
            pub inline fn perform(completion: *Completion, loop: *Loop) void {
                return @call(.auto, completion.CallbackFn(), .{ loop, completion });
            }

            /// Returns the metadata (or a pointer to it) for the desired operation.
            pub inline fn getMetadata(completion: *Completion, comptime op: OperationType, comptime as_ptr: bool) blk: {
                const Type = @FieldType(Operation, @tagName(op));
                break :blk if (as_ptr) *Type else Type;
            } {
                if (comptime as_ptr) {
                    return @ptrCast(&completion.metadata);
                }

                return @field(completion.metadata, @tagName(op));
            }

            pub inline fn castUserdata(completion: *const Completion, comptime T: type) *align(@alignOf(T)) T {
                return @ptrCast(@alignCast(completion.userdata));
            }

            /// Whether or not the completion is in the works.
            pub inline fn isAvailable(completion: *const Completion) bool {
                return completion.status == .idle;
            }

            /// Interprets `result` as a POSIX error.
            pub inline fn err(completion: *const Completion) posix.E {
                return @enumFromInt(-completion.result);
            }

            // operation kind
            pub const OperationType = enum(u8) {
                none,
                read,
                write,
                writev,
                connect,
                accept,
                recv,
                send,
                timeout,
                //cancel,
                close,

                /// Returns the return type for the given operation type.
                /// Can be run on comptime, but not necessarily.
                pub fn Returns(op: OperationType) type {
                    return switch (op) {
                        .none => unreachable,
                        .read => ReadError!usize,
                        .write => WriteError!usize,
                        .writev => WriteError!usize,
                        .connect => ConnectError!void,
                        .accept => AcceptError!Socket,
                        .recv => RecvError!usize,
                        .send => SendError!usize,
                        .timeout => TimeoutError!void,
                        .close => void,
                    };
                }
            };

            // Data needed by operations.
            pub const Operation = extern union {
                // default
                none: void,
                read: extern struct {
                    base: [*]u8,
                    len: usize,
                    handle: linux.fd_t,
                    __pad: u32 = 0,
                },
                write: extern struct {
                    base: [*]const u8,
                    len: usize,
                    handle: linux.fd_t,
                    __pad: u32 = 0,
                },
                writev: extern struct {
                    iovecs: [*]const posix.iovec,
                    nr_vecs: u32,
                    handle: linux.fd_t,
                },
                connect: extern struct {
                    addr: posix.sockaddr = undefined, // 16
                    socket: linux.fd_t,
                    socklen: posix.socklen_t,
                },
                accept: extern struct {
                    addr: posix.sockaddr = undefined, // 16
                    socket: linux.fd_t, // listener socket
                    socklen: posix.socklen_t = @sizeOf(posix.sockaddr),
                },
                recv: extern struct {
                    base: [*]u8,
                    len: usize,
                    socket: linux.fd_t,
                    flags: u32,
                },
                send: extern struct {
                    base: [*]const u8,
                    len: usize,
                    socket: linux.fd_t,
                    flags: u32,
                },
                timeout: extern struct {
                    timespec: linux.kernel_timespec, // 16
                    __pad: u64 = 0,
                },
                close: extern struct {
                    handle: HandleOrSocket, // 4
                    __pad: [20]u8 = undefined,
                },
            };
        };

        // on unix, there's a unified fd type that's used for both sockets and file, pipe etc. handles.
        // since this library is intended for cross platform usage and windows separates file descriptors by different kinds, I've decided to adapt the same mindset to here.
        pub const Handle = linux.fd_t;
        pub const Socket = linux.fd_t;
    };
}

test "io_uring: direct descriptors" {
    const DefaultLoop = LoopImpl(.{
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
