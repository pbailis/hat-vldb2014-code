package edu.berkeley.thebes.twopl.server;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLSlaveReplicaService;

public class TwoPLSlaveServiceHandler implements TwoPLSlaveReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;

    public TwoPLSlaveServiceHandler(IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    @Override
    public void put(String key, ThriftDataItem value) throws TException {
        persistenceEngine.put_if_newer(key, new DataItem(value));
    }
}