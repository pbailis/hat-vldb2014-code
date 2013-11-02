#!/usr/local/bin/thrift --gen java

namespace java edu.berkeley.thebes.common.thrift

struct ThriftVersion {
  1: required i64 version,
}
