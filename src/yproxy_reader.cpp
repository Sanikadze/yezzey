#include "yproxy_reader.h"
#include "gucs.h"
#include "yproxy_io.h"

#include <errno.h>

extern "C" {
#include "postgres.h"
#include "miscadmin.h"
}

const int kDefaultRetryLimit = 100;

YProxyReader::YProxyReader(std::shared_ptr<IOadv> adv, ssize_t segindx,
                           const std::vector<ChunkInfo> order)
    : YProxyConnector(adv, segindx), order_ptr_(0), order_(order),
      current_chunk_remaining_bytes_(0), current_retry(0),
      retry_limit(kDefaultRetryLimit) {}

YProxyReader::~YProxyReader() { close(); }

bool YProxyReader::close() { return YProxyConnector::close(); }

std::vector<char> YProxyReader::ConstructCatRequest(const ChunkInfo &ci,
                                                    size_t start_off) {

  uint64_t settingsCnt = 1;
  std::vector<std::pair<std::string, std::string>> settings = {
      {"TableSpace", adv_->tableSpace},
  };

  MsgBuilder builder = MsgBuilder()
                           .fieldProto()
                           .fieldString(ci.x_path.size())
                           .fieldUInt64() // offset
                           .fieldUInt64();

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.fieldString(settings[j].first.size())
        .fieldString(settings[j].second.size());
  }
  builder.endDescription();

  builder
      .addProto(MessageTypeCatV2, ci.enc ? DecryptRequest : NoDecryptRequest,
                ci.kek ? UseKEK : NoUseKEK)
      .addString(ci.x_path)
      .addUInt64(start_off)
      .addUInt64(settingsCnt);

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.addString(settings[j].first).addString(settings[j].second);
  }

  return builder.get();
}

int YProxyReader::prepareYproxyConnection(const ChunkInfo &ci,
                                          size_t start_off) {
  int rb = YProxyConnector::prepareYproxyConnection();
  if (rb != 0) {
    return rb;
  }

  auto msg = ConstructCatRequest(ci, start_off);

  if (commonWriteFull(client_fd_, msg) == -1) {
    return -1;
  }

  /* reset retry count */
  this->current_retry = 0;

  // now we are ready to read our request data
  return client_fd_;
}

bool YProxyReader::read(char *buffer, size_t *amount) {
  // preparing done, read data

  while (1) {
    CHECK_FOR_INTERRUPTS();
    if (current_chunk_remaining_bytes_ == 0) {
      // no more data to read
      if (order_ptr_ == order_.size()) {
        *amount = 0;
        return false;
      }

      // close previous read socket, if any
      if (!this->close()) {
        // wtf?
        return false;
      }
      auto rc = this->prepareYproxyConnection(order_[order_ptr_], 0);
      if (rc < 0) {
        if (++this->current_retry >= this->retry_limit) {
          this->close();
          ereport(ERROR,
                  (errcode(ERRCODE_IO_ERROR),
                   errmsg("yezzey: failed to connect to yproxy after %d retries for chunk %lu",
                          retry_limit, (unsigned long)order_ptr_)));
        }
        pg_usleep(1000000L); /* 1 second */
        continue;
      }
      current_chunk_offset_ = 0;
      current_chunk_remaining_bytes_ = order_[order_ptr_].size;
    }

    /* Cap read to remaining chunk bytes to prevent reading past boundary */
    size_t to_read = *amount;
    if ((int64_t)to_read > current_chunk_remaining_bytes_) {
      to_read = current_chunk_remaining_bytes_;
    }
    auto rc = yproxy_read_with_interrupts(client_fd_, buffer, to_read,
                                           yproxy_socket_timeout);
    if (rc <= 0) {
      int saved_errno = errno;

      if (saved_errno == EINTR) {
        /* PostgreSQL cancel/terminate - do not retry, propagate immediately */
        this->close();
        ereport(ERROR,
                (errcode(ERRCODE_QUERY_CANCELED),
                 errmsg("yezzey: read interrupted by cancel request on offset %lu",
                        current_chunk_offset_)));
      } else if (saved_errno == ETIMEDOUT) {
        /* Timeout - do not retry, return error */
        this->close();
        ereport(ERROR,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg("yezzey: read timeout after %d seconds on offset %lu",
                        yproxy_socket_timeout, current_chunk_offset_)));
      } else {
        elog(WARNING, "reacquiring connection on offset %lu",
             current_chunk_offset_);
      }

      this->close(); /* close broken socket before reconnecting */
      if (++this->current_retry < this->retry_limit) {
        auto rrc = this->prepareYproxyConnection(order_[order_ptr_],
                                                 current_chunk_offset_);
        if (rrc < 0) {
          pg_usleep(1000000L); /* 1 second */
          continue;
        }
      } else {
        // error, and we are out of retries.
        this->close();
        ereport(ERROR,
                (errcode(ERRCODE_IO_ERROR),
                 errmsg("yezzey: failed to read from external storage after %d retries on offset %lu",
                        retry_limit, current_chunk_offset_)));
      }
      continue;
    }
    current_chunk_remaining_bytes_ -= rc;
    current_chunk_offset_ += rc;
    if (current_chunk_remaining_bytes_ == 0) {
      ++order_ptr_;
    }
    *amount = rc;

    return true;
  }
}

bool YProxyReader::empty() {
  return order_ptr_ == order_.size() && current_chunk_remaining_bytes_ <= 0;
};
