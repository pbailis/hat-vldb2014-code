#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"
include "version.thrift"

namespace java edu.berkeley.thebes.hat.common.thrift

struct ThriftDataDependency {
  1: string key,
  2: version.ThriftVersion version
}

struct DataDependencyRequest {
  1: i32 serverId,
  2: i64 requestId,
  3: ThriftDataDependency dependency
}

service ReplicaService {
  dataitem.ThriftDataItem get(1: string key
                        2: version.ThriftVersion requiredVersion);

  bool put(1: string key,
           2: dataitem.ThriftDataItem value);
}

service AntiEntropyService {
  oneway void put(1: list<string> key,
                  2: list<dataitem.ThriftDataItem> value);
                                         
  oneway void ackTransactionPending(1: binary transactionIds)
}
