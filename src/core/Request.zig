const std = @import("std");
const hparse = @import("hparse");
const Method = hparse.Method;
const Version = hparse.Version;
const Header = hparse.Header;
const parseRequest = hparse.parseRequest;
const ParseRequestError = hparse.ParseRequestError;

/// Represents a single HTTP request.
const Request = @This();
/// Requested method from the peer.
method: Method = .unknown,
/// Requested path from the peer.
path: ?[]const u8 = null,
/// Requested HTTP version, initially set to 1.0.
version: Version = .@"1.0",
/// HTTP headers sent from peer.
/// We only accept 32 headers at most.
headers: [32]Header = undefined,
/// Length of HTTP headers.
h_count: usize = 0,

/// Internal function.
/// Parses an HTTP request out of given slice.
/// Returns how many bytes are consumed from the start of the slice.
pub inline fn parse(req: *Request, slice: []const u8) ParseRequestError!usize {
    return parseRequest(slice, &req.method, &req.path, &req.version, &req.headers, &req.h_count);
}

/// Returns true if the connection is HTTP/1.1.
pub inline fn isHttp11(req: *const Request) bool {
    return req.version == .@"1.1";
}

/// Returns the number of headers received.
pub inline fn headerCount(req: *const Request) usize {
    return req.h_count;
}

/// Returns the header value for the given header key.
pub fn getHeader(req: *const Request, key: []const u8) ![]const u8 {
    for (req.headers[0..req.headerCount()]) |h| {
        if (std.mem.eql(u8, h.key, key)) {
            return h.value;
        }
    }

    return error.NotFound;
}
