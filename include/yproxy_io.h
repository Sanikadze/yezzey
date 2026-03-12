#pragma once

#include <stddef.h>
#include <sys/types.h>

/*
 * poll()-based I/O wrappers for yproxy socket communication.
 *
 * These replace raw ::read()/::write() calls to provide:
 * - Operation-level timeouts via poll()
 * - PostgreSQL interrupt checking (pg_cancel_backend support)
 * - Proper errno reporting (ETIMEDOUT on timeout, EINTR on cancel)
 */

/*
 * Read from fd with poll()-based timeout and interrupt checking.
 * Polls with 1-second intervals, checking for PostgreSQL interrupts
 * (QueryCancelPending, ProcDiePending) between polls.
 *
 * Returns bytes read on success, -1 on error.
 * errno is set to:
 *   ETIMEDOUT - total timeout expired
 *   EINTR     - PostgreSQL interrupt pending (cancel/terminate)
 *   other     - poll() or read() error
 */
ssize_t yproxy_read_with_interrupts(int fd, char *buffer, size_t len,
                                    int timeout_sec);

/*
 * Write to fd with poll()-based timeout and interrupt checking.
 * Polls with 1-second intervals, checking for PostgreSQL interrupts.
 *
 * Returns bytes written on success, -1 on error.
 * errno conventions same as yproxy_read_with_interrupts.
 */
ssize_t yproxy_write_with_interrupts(int fd, const char *buffer, size_t len,
                                     int timeout_sec);
