# yezzey / yproxy Timeout Reference

## Timeout Hierarchy

```
GP statement_timeout (user-defined, e.g. 300s)
  └── yezzey socket timeout (yproxy_socket_timeout GUC, default 120s)
        └── yproxy S3 operation timeout (metadata 60s, stream 5m)
              └── yproxy HTTP transport (dial 10s, response header 30s)
                    └── yproxy internal retry (100 retries × ~1s, for S3 read errors)
```

For metadata operations: yproxy S3 timeout (60s) < yezzey socket (120s) — yproxy
returns an error to yezzey cleanly.

For streaming operations: yezzey socket (120s) < yproxy stream (5m) — if S3 stalls
mid-stream, yezzey closes the socket first; yproxy detects this and cancels the S3
operation via context cancellation.

## yezzey Timeouts (PostgreSQL GUC)

### yezzey.yproxy_socket_timeout

| Property | Value |
|----------|-------|
| Type | integer (seconds) |
| Default | 120 |
| Min | 0 (disabled) |
| Max | 3600 |
| Context | PGC_SUSET (superuser SET) |
| File | `yezzey.c`, `gucs.h` |

Controls the maximum time yezzey waits for a response from yproxy on the Unix socket.
Applied at two levels:

1. **SO_RCVTIMEO / SO_SNDTIMEO** — kernel-level socket timeout set via `setsockopt()`
   in `prepareYproxyConnection()`. If the socket is idle for this duration, `read()`
   or `write()` returns -1 with `errno = EAGAIN`.

2. **poll() loop total timeout** — the `yproxy_read_with_interrupts()` and
   `yproxy_write_with_interrupts()` functions in `yproxy_io.cpp` use this as the
   total timeout across all poll iterations.

**Why 120 seconds?**
- yproxy's internal retry for S3 reads: 100 retries × ~1s = ~100 seconds.
- The socket timeout should exceed yproxy's retry window so yproxy has a chance to
  return a proper error rather than having yezzey cut the connection prematurely.
- 120s provides a 20-second buffer above yproxy's ~100s retry window.

**Setting to 0** disables socket timeouts entirely (not recommended — reverts to
infinite hang behavior).

## yproxy Timeouts (server-side)

### yproxy S3 Read Retry

| Property | Value |
|----------|-------|
| Location | `pkg/proc/yio/yrreader.go` |
| Retry limit | 100 (hardcoded `defaultRetryLimit`) |
| Sleep between retries | 1 second (with random jitter) |
| Total retry window | ~100 seconds |

When `YRetryReader.Read()` encounters an S3 read error:
1. Calls `Restart(offsetReached)` to reopen the S3 object at the current offset.
2. Uses S3 `Range: bytes=offset-` header for efficient resumption.
3. Sleeps 1 second between retries (with random jitter to mitigate overload).

This retry is transparent to yezzey — the Unix socket stays open during retries.

### yproxy S3 Delete Retry

| Property | Value |
|----------|-------|
| Location | `pkg/proc/delete_handler.go` |
| Retry limit | 10 |
| Strategy | Retry remaining failed objects |

`HandleDeleteGarbage` and `HandleDelete2Prefix` retry batch deletes up to 10 times
for objects that failed to delete.

### yproxy S3 HTTP Timeouts

yproxy configures a custom `http.Transport` with explicit timeouts for S3 communication
(`pkg/storage/sessionpool.go`):

| Setting | Default | Description |
|---------|---------|-------------|
| `s3_dial_timeout` | 10s | TCP connection establishment |
| `s3_response_header_timeout` | 30s | Wait for response headers after request sent |
| `s3_idle_conn_timeout` | 90s | Idle connection pool lifetime |

Operation-level context deadlines are applied to each S3 API call:

| Setting | Default | Description |
|---------|---------|-------------|
| `s3_operation_timeout` | 60s | Metadata ops: list, delete, head, copy |
| `s3_stream_timeout` | 5m | Streaming ops: get (download), put (upload) |

A background health-check goroutine probes S3 every 30s via `HeadBucket` and
exposes `yproxy_s3_healthy` (gauge 0/1) on the Prometheus metrics port (default 2112).

## yezzey Read Path Retry

| Property | Value |
|----------|-------|
| Location | `src/yproxy_reader.cpp` |
| Retry limit | 100 (`kDefaultRetryLimit`) |
| Sleep between retries | 1 second (`pg_usleep(1000000L)`) |
| Total retry window | ~100 seconds |

This is a SEPARATE retry from yproxy's internal retry. yezzey retry triggers when
the Unix socket to yproxy breaks (yproxy crash, restart, or socket error). On each
retry:
1. Close the broken socket.
2. `prepareYproxyConnection()` — open new Unix socket to yproxy.
3. Send CatV2 message with `start_off = current_chunk_offset_`.
4. yproxy starts a new goroutine and streams from S3 at the requested offset.

**Error classification**:

| errno | Action | Rationale |
|-------|--------|-----------|
| EINTR | Fail immediately | User cancel (pg_cancel_backend) |
| ETIMEDOUT | Fail immediately | Socket timeout — yproxy unresponsive |
| ECONNRESET | Retry | Connection broken, yproxy may recover |
| EPIPE | Retry | yproxy closed socket |
| ECONNREFUSED | Retry | yproxy restarting |
| 0 (EOF) | Retry | Disconnect |

## GP statement_timeout

PostgreSQL's `statement_timeout` (user-configurable) applies at the query level.
If set, it fires independently of yezzey timeouts and cancels the backend via
the standard interrupt mechanism.

For yezzey operations to respond to `statement_timeout`, the I/O path must check
`InterruptPending` — which is now done in the poll() loop.

## Timeout Interaction Diagram

```
Time →

SELECT from offloaded table:

  [GP backend]                    [yproxy]                    [S3]
       |                              |                          |
       |--- Unix socket connect ----->|                          |
       |--- CatV2 (offset=0) ------->|                          |
       |                              |--- GetObject ---------->|
       |                              |                          |
       |    poll(1s) ←─── InterruptPending check                 |
       |    poll(1s) ←─── InterruptPending check                 |
       |                              |<--- data stream ---------|
       |<--- data -------------------|                           |
       |                              |                          |
       |    ... S3 stops responding ...                          |
       |                              |                          |
       |    poll(1s) ←─── InterruptPending check                 |
       |    poll(1s) ←─── InterruptPending check                 |
       |    ...                       |    (yproxy retry 1/100)  |
       |    ...                       |    (yproxy retry 2/100)  |
       |    ...                       |    ...                   |
       |    ...                       |    (yproxy retry 100/100)|
       |                              |                          |
       |    If yproxy returns error on socket:                   |
       |    → yezzey retry 1/100 (reconnect)                     |
       |                              |                          |
       |    If yproxy_socket_timeout (120s) reached:             |
       |    → errno=ETIMEDOUT                                    |
       |    → ereport(ERROR, "socket timeout")                   |
       |                              |                          |
       |    If pg_cancel_backend():                              |
       |    → InterruptPending detected in poll loop             |
       |    → errno=EINTR, return error                          |
       |    → CHECK_FOR_INTERRUPTS() at C boundary               |
```
