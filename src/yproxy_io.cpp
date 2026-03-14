#include "yproxy_io.h"
#include "gucs.h"

#include <errno.h>
#include <poll.h>
#include <unistd.h>

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

/* Poll interval in milliseconds for interrupt checking */
#define YPROXY_POLL_INTERVAL_MS 1000

ssize_t yproxy_read_with_interrupts(int fd, char *buffer, size_t len,
                                    int timeout_sec) {
  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLIN;

  int remaining_sec = timeout_sec;

  while (true) {
    /*
     * Check for PostgreSQL interrupts (pg_cancel_backend, SIGTERM).
     * We check the flags directly instead of calling CHECK_FOR_INTERRUPTS()
     * to avoid longjmp through C++ stack frames. The caller on the C boundary
     * should call CHECK_FOR_INTERRUPTS() when we return an error.
     */
    if (InterruptPending) {
      errno = EINTR;
      return -1;
    }

    int ret = poll(&pfd, 1, YPROXY_POLL_INTERVAL_MS);
    if (ret < 0) {
      if (errno == EINTR)
        continue; /* signal interrupted poll, retry */
      return -1;  /* real error */
    }
    if (ret == 0) {
      /* poll timeout - check if total timeout expired */
      if (timeout_sec > 0) {
        remaining_sec--;
        if (remaining_sec <= 0) {
          errno = ETIMEDOUT;
          return -1;
        }
      }
      continue;
    }

    /* Data ready - perform the read (check POLLIN first, since POLLHUP
     * may be set simultaneously when the remote end closed after writing) */
    if (pfd.revents & POLLIN) {
      return ::read(fd, buffer, len);
    }

    /* No data and socket error/hangup */
    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
      errno = ECONNRESET;
      return -1;
    }
  }
}

ssize_t yproxy_write_with_interrupts(int fd, const char *buffer, size_t len,
                                     int timeout_sec) {
  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLOUT;

  int remaining_sec = timeout_sec;

  while (true) {
    if (InterruptPending) {
      errno = EINTR;
      return -1;
    }

    int ret = poll(&pfd, 1, YPROXY_POLL_INTERVAL_MS);
    if (ret < 0) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    if (ret == 0) {
      if (timeout_sec > 0) {
        remaining_sec--;
        if (remaining_sec <= 0) {
          errno = ETIMEDOUT;
          return -1;
        }
      }
      continue;
    }

    /* Check writability first, then errors */
    if (pfd.revents & POLLOUT) {
      return ::write(fd, buffer, len);
    }

    if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
      errno = ECONNRESET;
      return -1;
    }
  }
}
