package edu.berkeley.thebes.twopl.server;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.server.TwoPLLocalLockManager.LockType;

public class TwoPLMasterServiceHandler implements TwoPLMasterReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private TwoPLLocalLockManager lockManager;
    private TwoPLSlaveReplicationService slaveReplicationService;

    public TwoPLMasterServiceHandler(IPersistenceEngine persistenceEngine,
            TwoPLLocalLockManager lockManager,
            TwoPLSlaveReplicationService slaveReplicationService) {
        this.persistenceEngine = persistenceEngine;
        this.lockManager = lockManager;
        this.slaveReplicationService = slaveReplicationService;
    }

    @Override
    public void write_lock(long sessionId, String key) throws TException {
        lockManager.lock(LockType.WRITE, key, sessionId);
    }
    
    @Override
    public void read_lock(long sessionId, String key) throws TException {
        lockManager.lock(LockType.READ, key, sessionId);
    }

    @Override
    public void unlock(long sessionId, String key) throws TException {
        lockManager.unlock(key, sessionId);
    }

    @Override
    public ThriftDataItem get(long sessionId, String key) throws TException {
        if (lockManager.ownsLock(LockType.READ, key, sessionId)) {
            return persistenceEngine.get(key).toThrift();
        } else {
            throw new TException("Session " + sessionId + "does not own GET lock on '" + key + "'");
        }
    }

    @Override
    public boolean put(long sessionId, String key, ThriftDataItem value) throws TException {
        if (lockManager.ownsLock(LockType.WRITE, key, sessionId)) {
            persistenceEngine.put_if_newer(key, new DataItem(value));
            slaveReplicationService.sendToSlaves(key, value);
            return true;
        } else {
            throw new TException("Session " + sessionId + " does not own PUT lock on '" + key + "'");
        }
    }

    @Override
    public boolean unsafe_load(String key, ThriftDataItem value)
            throws TException {
        persistenceEngine.put_if_newer(key, new DataItem(value));
        slaveReplicationService.sendToSlaves(key, value);
        return true;
    }
}