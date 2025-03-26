//! io_uring extensions zig doesn't support yet.

const std = @import("std");
const os = std.os;
const assert = std.debug.assert;
const posix = std.posix;
const linux = os.linux;
const IO_Uring = linux.IoUring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;

/// If set, buffers consumed from this buffer ring can be
/// consumed incrementally. Normally one (or more) buffers
/// are fully consumed. With incremental consumptions, it's
/// feasible to register big ranges of buffers, and each
/// use of it will consume only as much as it needs. This
/// requires that both the kernel and application keep
/// track of where the current read/recv index is at.
///
/// Linux 6.12+.
pub const IOU_PBUF_RING_INC = 2;

pub inline fn io_uring_prep_cancel_fd(sqe: *io_uring_sqe, fd: linux.fd_t, flags: u32) void {
    sqe.prep_rw(.ASYNC_CANCEL, fd, 0, 0, 0);
    sqe.rw_flags = flags | linux.IORING_ASYNC_CANCEL_FD;
}

pub inline fn io_uring_prep_files_update(sqe: *io_uring_sqe, fds: []const linux.fd_t, offset: u32) void {
    sqe.prep_rw(.FILES_UPDATE, -1, @intFromPtr(fds.ptr), fds.len, @intCast(offset));
}

/// FIXME: implementation here doesn't match to liburing, might want to change that.
/// https://github.com/axboe/liburing/blob/76bb80a36107e3808c4770c8112583813a4e511b/src/register.c#L139
pub fn io_uring_register_files_sparse(ring: *IO_Uring, nr: u32) !void {
    const reg = &linux.io_uring_rsrc_register{
        .nr = nr,
        .flags = linux.IORING_RSRC_REGISTER_SPARSE,
        .resv2 = 0,
        .data = 0,
        .tags = 0,
    };

    const res = linux.io_uring_register(
        ring.fd,
        .REGISTER_FILES2,
        @ptrCast(reg),
        @as(u32, @sizeOf(linux.io_uring_rsrc_register)),
    );

    return switch (linux.E.init(res)) {
        .SUCCESS => {},
        // One or more fds in the array are invalid, or the kernel does not support sparse sets:
        .BADF => error.FileDescriptorInvalid,
        .BUSY => error.FilesAlreadyRegistered,
        .INVAL => error.FilesEmpty,
        // Adding `nr_args` file references would exceed the maximum allowed number of files the
        // user is allowed to have according to the per-user RLIMIT_NOFILE resource limit and
        // the CAP_SYS_RESOURCE capability is not set, or `nr_args` exceeds the maximum allowed
        // for a fixed file set (older kernels have a limit of 1024 files vs 64K files):
        .MFILE => error.UserFdQuotaExceeded,
        // Insufficient kernel resources, or the caller had a non-zero RLIMIT_MEMLOCK soft
        // resource limit but tried to lock more memory than the limit permitted (not enforced
        // when the process is privileged with CAP_IPC_LOCK):
        .NOMEM => error.SystemResources,
        // Attempt to register files on a ring already registering files or being torn down:
        .NXIO => error.RingShuttingDownOrAlreadyRegisteringFiles,
        else => |errno| posix.unexpectedErrno(errno),
    };
}

/// The implementation for io_uring_setup_buf_ring doesn't match our needs, we want to set flags too.
/// So an alternative is provided here.
/// FIXME: Flags are given as u32 but io_uring_buf_reg actually expects u16 in it's flags field.
/// This is ported the same as liburing and might change in the future.
pub fn io_uring_setup_buf_ring(ring: *IO_Uring, entries: u16, group_id: u16, flags: u32) !*align(std.heap.page_size_min) linux.io_uring_buf_ring {
    if (entries == 0 or entries > 1 << 15) return error.EntriesNotInRange;
    if (!std.math.isPowerOfTwo(entries)) return error.EntriesNotPowerOfTwo;

    const mmap_size = @as(usize, entries) * @sizeOf(linux.io_uring_buf);
    const mmap = try posix.mmap(
        null,
        mmap_size,
        posix.PROT.READ | posix.PROT.WRITE,
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        -1,
        0,
    );
    errdefer posix.munmap(mmap);
    assert(mmap.len == mmap_size);

    const br: *align(std.heap.page_size_min) linux.io_uring_buf_ring = @ptrCast(mmap.ptr);

    // Same as linux.io_uring_buf_reg but `pad` field name replace with flags.
    const io_uring_buf_reg_flags = extern struct {
        ring_addr: u64,
        ring_entries: u32,
        bgid: u16,
        flags: u16,
        resv: [3]u64,
    };

    // register_buf_ring
    var reg = std.mem.zeroInit(io_uring_buf_reg_flags, .{
        .ring_addr = @intFromPtr(br),
        .ring_entries = entries,
        .bgid = group_id,
        .flags = @as(u16, @intCast(flags)),
    });

    const res = linux.io_uring_register(ring.fd, .REGISTER_PBUF_RING, @ptrCast(&reg), 1);

    // handle_register_buf_ring_result
    switch (linux.E.init(res)) {
        .SUCCESS => {},
        .INVAL => return error.ArgumentsInvalid,
        else => |errno| return posix.unexpectedErrno(errno),
    }

    return br;
}
