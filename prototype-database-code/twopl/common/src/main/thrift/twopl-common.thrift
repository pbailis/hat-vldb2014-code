#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"

namespace java edu.berkeley.thebes.twopl.common.thrift

service TwoPLMasterReplicaService {
  void write_lock(1: i64 sessionId, 2: string key);
  void read_lock(1: i64 sessionId, 2: string key);
  void unlock(1: i64 sessionId, 2: string key);
  dataitem.ThriftDataItem get(1: i64 sessionId, 2: string key);
  bool put(1: i64 sessionId, 2: string key, 3: dataitem.ThriftDataItem value);
  
  bool unsafe_load(1: string key, 2: dataitem.ThriftDataItem value);
}

service TwoPLSlaveReplicaService {
  oneway void put(1: string key, 2: dataitem.ThriftDataItem value);
}

struct TwoPLTransactionResult {
  2: map<string, binary> requestedValues; # <key, value> of final value of GET'd items
}

service TwoPLTransactionService {
  # A transaction is a list of operations. Operations are defined in TODO.
  TwoPLTransactionResult execute(1: list<string> transaction);
}