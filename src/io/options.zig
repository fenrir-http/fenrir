/// Which backend to use for I/O related tasks.
pub const IoEngine = enum {
    /// Modern asynchronous I/O API of Linux.
    io_uring,
    /// Completion-based Overlapped I/O API of Windows.
    iocp,
};

/// Options shared across io models.
pub const Options = struct {
    /// TODO: zero-copy send
    /// TODO: investigate SQPOLL
    /// TODO: investigate `io_uring_prep_fixed_fd_install`.
    /// TODO: investigate `io_uring_register_ring_fd`.
    /// TODO: investigate `IOU_PBUF_RING_MMAP`.
    /// io_uring specific options.
    io_uring: packed struct {
        /// Whether or not direct descriptors of io_uring be used.
        ///
        /// Direct descriptors exist only within the ring itself, but can be used for any request within that ring.
        /// https://kernel.dk/axboe-kr2022.pdf
        ///
        /// Beware that code that set this flag are likely to be non-portable. Since it activates Linux specific APIs.
        ///
        /// If this option is activated, file descriptors can only be used after they've been registered to loop via:
        /// `initDescriptors`, `updateDescriptors` or `updateFds`.
        ///
        /// TODO: Support `io_uring_prep_socket`, `io_uring_prep_open` etc. functions for direct descriptors.
        direct_descriptors_mode: bool = false,
        /// FIXME: Currently buggy, don't use it.
        /// Experimental.
        ///
        /// If this flag is activated, send operations will be done in zero-copy fashion.
        /// Zero-copy is not guaranteed though, io_uring may decide to copy the buffer.
        ///
        /// Issue the zerocopy equivalent of a send(2) system call.
        /// Similar to IORING_OP_SEND, but tries to avoid making
        /// intermediate copies of data. Zerocopy execution is not
        /// guaranteed and may fall back to copying. The request may
        /// also fail with -EOPNOTSUPP, when a protocol doesn't support
        /// zerocopy, in which case users are recommended to use
        /// copying sends instead.
        /// https://man7.org/linux/man-pages/man2/io_uring_enter.2.html
        /// https://man7.org/linux/man-pages/man3/io_uring_prep_send_zc.3.html
        zero_copy_sends: bool = false,
    } = .{},
    // TODO:
    iocp: struct {} = .{},

    /// Whether or not direct descriptors mode is activated.
    /// Only supported for io_uring.
    pub inline fn directDescriptors(options: *const Options) bool {
        return options.io_uring.direct_descriptors_mode;
    }
};
