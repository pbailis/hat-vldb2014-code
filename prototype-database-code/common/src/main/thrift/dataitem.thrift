#!/usr/local/bin/thrift --gen java

namespace java edu.berkeley.thebes.common.thrift

include "version.thrift"

struct ThriftDataItem {
  1: binary data,
  2: version.ThriftVersion version,
  4: optional list<string> transactionKeys
}
