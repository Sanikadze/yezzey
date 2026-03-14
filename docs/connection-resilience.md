# yezzey: Connection Resilience and Error Handling

## Overview

yezzey is a custom SMGR (storage manager) for Greenplum that replaces local file I/O
with network calls through a Unix socket to yproxy, which in turn communicates with S3.

Unlike standard `mdsmgr` where `read()`/`write()` operate on local files with OS-level
guarantees, yezzey's I/O chain (`Unix socket → yproxy → HTTP → S3`) has multiple failure
points: S3 unavailability, yproxy hangs, broken Unix sockets.

This document describes how yezzey handles connection failures and the design decisions
behind the resilience mechanisms.

## SMGR Error Contract

From `src/include/storage/smgr.h`:

> "smgr subfunctions are generally expected to report problems via elog(ERROR).
> An exception is that smgr_unlink should use elog(WARNING), rather than erroring out,
> because we normally unlink relations during post-commit/abort cleanup."

Key rules:
- SMGR functions do NOT return error codes. They raise `ereport(ERROR)` on failure.
- `smgr_unlink`: uses `ereport(WARNING)`, not ERROR.
- `smgr_exists`: returns bool (exception).
- `mdsmgr` uses `errcode_for_file_access()` for I/O errors and `ERRCODE_DATA_CORRUPTED`
  for short reads.

For yezzey, the recommended errcode for network errors is `ERRCODE_IO_ERROR` (class XX).
`errcode_for_file_access()` maps `ETIMEDOUT` to `ERRCODE_IO_ERROR` automatically.

## Error Semantics by Operation Type

### READ (SELECT)

`ereport(ERROR)` on timeout is **safe**:
- AO tables use palloc'd `BufferedRead` buffers, NOT the shared buffer pool.
- No shared buffer descriptors, dirty flags, or BM locks are involved.
- Transaction abort triggers automatic memory context cleanup.
- No shared state is modified during read operations.

**Retry strategy**: `yproxy_reader.cpp` retries on connection errors (up to 100 attempts,
1s between retries) by reconnecting to yproxy with the current offset. yproxy uses S3
Range requests internally to resume. AO block-level checksums protect data integrity
across retries.

When retries are exhausted: `ereport(ERROR, ERRCODE_IO_ERROR, "failed to read from
external storage after N retries")`.

### WRITE (INSERT / offload)

Write failures are **not retried** — this is by design:
- AO append-only semantics require sequential writes.
- Partial retry would create gaps or duplicate data.
- The correct recovery is to abort the upload and re-offload from the last known offset.

When write fails:
1. `ereport(ERROR)` aborts the transaction, rolling back any `virtual_index` metadata.
2. No data corruption in PostgreSQL (transactional rollback).
3. The multipart upload in S3 may be left orphaned (see below).

**Orphaned multipart uploads**: When a write breaks mid-stream, orphaned uploads may
persist in S3 until VACUUM (`HandleDeleteGarbage`). yproxy tracks whether a `CopyDone`
message was received before the socket closed; if not, it closes the pipe with an error
(`pw.CloseWithError()`), causing `s3manager.Upload()` to abort the upload rather than
completing it with partial data.

**Known issue — metadata ordering**: `virtual_index` is updated BEFORE `io_close()`
confirmation. If `io_close()` fails, the transaction rolls back (safe), but the ideal
order would be: `io_close()` first, then metadata update.

### LIST (metadata queries)

Used to enumerate chunks for a relation in S3.

**Critical bug (pre-fix)**: `list_relation_chunks()` returned an empty vector on network
error, indistinguishable from "no chunks found". The `retCode=-1` from `readMessage()`
was not checked by calling code.

**Fix**: `ereport(ERROR, ERRCODE_IO_ERROR)` on list failure. No retry — list operations
are idempotent and can be retried at a higher level.

### DELETE / UNLINK (DROP TABLE)

- GP 6.x: `yezzey_unlink()` is a NO-OP for offloaded tables (`spcNode == YEZZEYTABLESPACE_OID`).
  S3 data is NOT deleted on `DROP TABLE`.
- Modern yezzey: `mdunlink()` is called for local files only.
- `deleteChunk()` errors produce `elog(WARNING)` per SMGR contract (unlink uses WARNING).
- Partial deletes are possible with no rollback mechanism.

## Timeout and Interrupt Mechanisms

### poll()-based I/O with Interrupt Checking

All blocking `::read()`/`::write()` calls are wrapped in `yproxy_read_with_interrupts()`
and `yproxy_write_with_interrupts()` (defined in `src/yproxy_io.cpp`):

```
loop:
  check InterruptPending → if set, errno=EINTR, return -1
  poll(fd, 1 second timeout)
  if poll timeout → decrement total_timeout, continue
  if poll ready → ::read()/::write()
  if total_timeout exhausted → errno=ETIMEDOUT, return -1
```

Key design decisions:
- `InterruptPending` is checked (not `CHECK_FOR_INTERRUPTS()` directly) to avoid
  `longjmp` through C++ stack frames. This follows the gpcloud pattern
  (`S3QueryIsAbortInProgress()`).
- `CHECK_FOR_INTERRUPTS()` is called at the C boundary (smgr.c) after the C++ code
  returns an error.
- Poll interval is 1 second — provides cancel responsiveness within 1 second.

### pg_cancel_backend() Support

Before the resilience changes, `pg_cancel_backend()` only worked during the offload write
loop (`storage.cpp` had the only `CHECK_FOR_INTERRUPTS()` call). All other paths (read,
list, delete, writer close) were unresponsive to cancellation.

After changes, `pg_cancel_backend()` works on all I/O paths because:
1. `InterruptPending` is checked in the poll loop (every 1 second).
2. When detected, the I/O function returns with `errno = EINTR`.
3. Callers propagate the error up to the C boundary.
4. `CHECK_FOR_INTERRUPTS()` fires the actual longjmp from C code.

## Retry Strategy

### Read Path

| Parameter | Value | Notes |
|-----------|-------|-------|
| retry_limit | 100 | Reconnect attempts on connection error |
| retry_sleep | 1 second | Between reconnect attempts |
| Total retry window | ~100 seconds | Before giving up |

Retry triggers: `ECONNRESET`, `EPIPE`, `ECONNREFUSED`, read returning 0 (EOF).
No-retry: `EINTR` (cancel), `ETIMEDOUT` (socket timeout).

On each retry, yezzey reconnects to yproxy with the current chunk offset.
yproxy re-reads from S3 (discarding bytes before the offset for encrypted data,
or using Range requests for its internal retry).

### Write Path

No retry. One failed write aborts the operation. This is correct for AO append-only
semantics. The offload can be retried at a higher level; `yezzey_calc_virtual_relation_size()`
calculates the correct resume offset from completed chunks.

### List / Delete Paths

No retry. Errors are reported immediately.

## Data Consistency

### Offload Idempotency

Partially works:
- `yezzey_calc_virtual_relation_size()` reads all S3 objects to count completed bytes.
- Only completed chunks are counted (incomplete multipart uploads are invisible in
  `ListObjects`).
- Re-offload creates a new chunk with a different path (includes modcount and LSN).
- Old orphaned uploads persist until VACUUM.

### virtual_index Consistency

The `virtual_index` table uses standard PostgreSQL transactions:
- INSERT into `virtual_index` is part of the current transaction.
- On `ereport(ERROR)`, the transaction rolls back, undoing any metadata changes.
- Inconsistent state between `virtual_index` and S3 is prevented by transactional
  rollback.

## Architecture Decision

The implemented approach uses poll()-based I/O:

**poll() + InterruptPending check**:
Provides cancel support, granular timeouts, and interrupt responsiveness.
Follows the gpcloud pattern for safe C++/PostgreSQL interaction.

**Future improvement — WaitLatchOrSocket (PostgreSQL-native)**:
`WaitLatchOrSocket()` with `WL_LATCH_SET | WL_SOCKET_READABLE | WL_TIMEOUT |
WL_POSTMASTER_DEATH` would add native integration with GP signals and postmaster
death detection. Confirmed available in GP 6.x (`latch.h`). Precedent: `postgres_fdw`
uses `WaitLatchOrSocket` from extension code.
