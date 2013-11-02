package edu.berkeley.thebes.common.persistence.util;

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {

    private class LockWaiting {
        private long numWaiters;
        private Condition condition;

        public LockWaiting(Condition condition) {
            numWaiters = 0;
            this.condition = condition;
        }

        public boolean isEmpty() {
            return numWaiters == 0;
        }

        public void signalWaiter() {
            assert(numWaiters > 0);
            condition.signal();
        }

        public void waitInterruptibly() {
            numWaiters++;
            condition.awaitUninterruptibly();
            numWaiters--;
        }
    }

    public int getSize() {
        return lockTable.size();
    }

    private Map<String, LockWaiting> lockTable;
    private Lock tableLock = new ReentrantLock();

    public LockManager() {
        lockTable = Maps.newHashMap();
    }

    public void lock(String key) {
        tableLock.lock();
        if(!lockTable.containsKey(key))
            lockTable.put(key, new LockWaiting(tableLock.newCondition()));
        else
            lockTable.get(key).waitInterruptibly();
        tableLock.unlock();
    }

    public void unlock(String key) {
        tableLock.lock();
        if(lockTable.get(key).isEmpty())
            lockTable.remove(key);
        else
            lockTable.get(key).signalWaiter();
        tableLock.unlock();
    }
}