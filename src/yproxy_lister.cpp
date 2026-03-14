#include "yproxy_lister.h"
#include "gucs.h"
#include "url.h"
#include "yproxy_io.h"

#include <errno.h>

/* ereport/elog available via yproxy_connector.h -> io_adv.h -> pg.h */

/*
 *
 *  Yproxy Lister
 */

YProxyLister::YProxyLister(std::shared_ptr<IOadv> adv, ssize_t segindx)
    : YProxyConnector(adv, segindx) {}

YProxyLister::~YProxyLister() { close(); }

bool YProxyLister::close() { return YProxyConnector::close(); }

int YProxyLister::prepareYproxyConnection() {
  // open unix data socket
  return YProxyConnector::prepareYproxyConnection();
}

std::vector<storageChunkMeta> YProxyLister::list_relation_chunks() {
  std::vector<storageChunkMeta> res;
  auto ret = prepareYproxyConnection();
  if (ret != 0) {
    this->close();
    ereport(ERROR,
            (errcode(ERRCODE_IO_ERROR),
             errmsg("yezzey: failed to connect to yproxy for listing chunks")));
  }

  auto msg = ConstructListRequest(yezzey_block_db_file_path(
      adv_->nspname, adv_->relname, adv_->coords_, segindx_));
  if (commonWriteFull(client_fd_, msg) == -1) {
    this->close();
    ereport(ERROR,
            (errcode(ERRCODE_IO_ERROR),
             errmsg("yezzey: failed to send list request to yproxy: %m")));
  }

  std::vector<storageChunkMeta> meta;
  while (true) {
    auto message = readMessage();
    if (message.retCode != 0) {
      this->close();
      ereport(ERROR,
              (errcode(ERRCODE_IO_ERROR),
               errmsg("yezzey: failed to read chunk list from yproxy: communication error")));
    }
    switch (message.type) {
    case MessageTypeObjectMeta:
      meta = readObjectMetaBody(&message.content);
      res.insert(res.end(), meta.begin(), meta.end());
      break;
    case MessageTypeReadyForQuery:
      return res;

    default:
      this->close();
      ereport(ERROR,
              (errcode(ERRCODE_IO_ERROR),
               errmsg("yezzey: unexpected message type %d from yproxy during listing",
                      message.type)));
    }
  }
}

std::vector<std::string> YProxyLister::list_chunk_names() {
  auto chunk_meta = list_relation_chunks();
  std::vector<std::string> res(chunk_meta.size());

  for (size_t i = 0; i < chunk_meta.size(); i++) {
    res[i] = chunk_meta[i].chunkName;
  }
  return res;
}

std::vector<char> YProxyLister::ConstructListRequest(std::string fileName) {

  uint64_t settingsCnt = 1;
  std::vector<std::pair<std::string, std::string>> settings = {
      {"TableSpace", adv_->tableSpace},
  };

  MsgBuilder builder =
      MsgBuilder().fieldProto().fieldString(fileName.size()).fieldUInt64();

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.fieldString(settings[j].first.size())
        .fieldString(settings[j].second.size());
  }
  builder.endDescription();

  builder.addProto(MessageTypeListV2)
      .addString(fileName)
      .addUInt64(settingsCnt);

  for (uint64_t j = 0; j < settingsCnt; ++j) {
    builder.addString(settings[j].first).addString(settings[j].second);
  }

  return builder.get();
}

YProxyLister::message YProxyLister::readMessage() {
  YProxyLister::message res;
  size_t len = MSG_HEADER_SIZE;
  char buffer[len];
  // try to read small number of bytes in one op
  // if failed, give up
  int rc = yproxy_read_with_interrupts(client_fd_, buffer, len,
                                       yproxy_socket_timeout);
  if (rc < 0 || (size_t)rc != len) {
    if (rc == -1 && errno == ETIMEDOUT) {
      elog(WARNING,
           "yproxy socket read timeout after %d seconds (list header)",
           yproxy_socket_timeout);
    } else if (rc == -1 && errno == EINTR) {
      elog(WARNING,
           "yproxy read interrupted by cancel request (list header)");
    }
    res.retCode = -1;
    return res;
  }

  uint64_t msgLen = 0;
  for (int i = 0; i < 8; i++) {
    msgLen <<= 8;
    msgLen += uint8_t(buffer[i]);
  }

  // substract header
  msgLen -= len;

  /* Sanity check: prevent excessive allocation from malformed messages */
  if (msgLen > 16 * 1024 * 1024) {
    elog(WARNING, "yezzey: yproxy list message too large: %lu bytes", msgLen);
    res.retCode = -1;
    return res;
  }

  /* Heap-allocate: msgLen can be large for list responses */
  std::vector<char> data(msgLen);
  /* Loop to handle short reads -- Unix sockets can return partial data
   * for large messages, similar to how commonWriteFull loops for writes */
  uint64_t total_read = 0;
  while (total_read < msgLen) {
    rc = yproxy_read_with_interrupts(client_fd_, data.data() + total_read,
                                     msgLen - total_read,
                                     yproxy_socket_timeout);
    if (rc < 0) {
      if (errno == ETIMEDOUT) {
        elog(WARNING,
             "yproxy socket read timeout after %d seconds (list body)",
             yproxy_socket_timeout);
      } else if (errno == EINTR) {
        elog(WARNING,
             "yproxy read interrupted by cancel request (list body)");
      }
      res.retCode = -1;
      return res;
    }
    if (rc == 0) {
      elog(WARNING, "yezzey: yproxy connection closed during list body read");
      res.retCode = -1;
      return res;
    }
    total_read += rc;
  }

  res.type = data[0];
  res.content = data;
  return res;
}

std::vector<storageChunkMeta>
YProxyLister::readObjectMetaBody(std::vector<char> *body) {
  std::vector<storageChunkMeta> res;
  size_t i = PROTO_HEADER_SIZE;
  while (i < body->size()) {
    std::vector<char> buff;
    while (i < body->size() && body->at(i) != 0) {
      buff.push_back(body->at(i));
      i++;
    }
    if (i >= body->size()) {
      ereport(ERROR,
              (errcode(ERRCODE_IO_ERROR),
               errmsg("yezzey: chunk metadata from yproxy missing null terminator at offset %zu",
                      i)));
    }
    i++; /* skip null terminator */
    std::string path(buff.begin(), buff.end());
    if (body->size() - i < 8) {
      ereport(ERROR,
              (errcode(ERRCODE_IO_ERROR),
               errmsg("yezzey: truncated chunk metadata from yproxy: "
                      "expected 8 bytes for size at offset %zu, have %zu",
                      i, body->size() - i)));
    }
    int64_t size = 0;
    for (size_t j = i; j < i + 8; j++) {
      size <<= 8;
      size += uint8_t(body->at(j));
    }
    i += 8;

    storageChunkMeta meta;
    meta.chunkName = path;
    meta.chunkSize = size;
    res.push_back(meta);
  }

  if (i != body->size()) {
    elog(WARNING, "yezzey: %zu trailing bytes in chunk metadata message",
         body->size() - i);
  }

  return res;
}
