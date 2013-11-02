package edu.berkeley.thebes.twopl.server;

import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import edu.berkeley.thebes.common.config.Config;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides an interface for acquiring read and write locks.
 * Note that locks cannot be upgraded, so the caller must acquire the highest level of
 * lock needed initially, or else break 2PL.
 */
public class TwoPLLocalLockManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(TwoPLLocalLockManager.class);

    private final Counter lockMetric = Metrics.newCounter(TwoPLLocalLockManager.class, "2pl-locks");

    private static final long requestTimeout = Config.getSocketTimeout();

    public enum LockType { READ, WRITE }
    
    private static class LockRequest implements Comparable<LockRequest> {
        private final LockType type;
        private final long sessionId;
        private final long timestamp;
        private final Condition condition;
        private final long wallClockCreationTime;

        private boolean valid; 
        
        public LockRequest(LockType type, long sessionId, long timestamp, Condition condition) {
            this.type = type;
            this.sessionId = sessionId;
            this.timestamp = timestamp;
            this.condition = condition;
            this.wallClockCreationTime = System.currentTimeMillis();
            this.valid = true;
        }
        
        public void sleep() {
            try {
                condition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        
        public void wake() {
            condition.signal();
        }
        
        public void invalidate() {
            valid = false;
        }
        
        public boolean equals(Object other) {
            if (!(other instanceof LockRequest)) {
                return false;
            }
            
            LockRequest otherRequest = (LockRequest) other;
            return Objects.equal(type, otherRequest.type) &&
                    Objects.equal(sessionId, otherRequest.sessionId);
        }

        @Override
        public int compareTo(LockRequest other) {
            return ComparisonChain.start()
                    .compare(timestamp, other.timestamp)
                    .compare(sessionId, other.sessionId)
                    .compare(type, other.type)
                    .result();
        }

        public boolean isValid() {
            return valid && (System.currentTimeMillis()-wallClockCreationTime < requestTimeout);
        }
        
        public String toString() {
            return String.format("[type=%s, session=%d, timestamp=%d, age=%d, valid=%b]", 
                    type, sessionId, timestamp, System.currentTimeMillis()-wallClockCreationTime,
                    valid);
        }
    }
    
    /**
     * Contains all the state related to a particular locked object.
     * This class supports multiple readers xor a single writer.
     */
    private static class LockState {
    	private boolean held;
    	private LockType mode;
        private Set<Long> lockers;
        
        private Lock lockLock = new ReentrantLock();
        private Set<LockRequest> queuedRequests;
        private AtomicLong logicalTime = new AtomicLong(0);
        
        public LockState() {
            this.held = false;
            this.lockers = new ConcurrentSkipListSet<Long>();
            this.queuedRequests = new ConcurrentSkipListSet<LockRequest>();
        }
        
        private boolean shouldGrantLock(LockRequest request) {
            // Reject all READ requests that come after some queued WRITE, and
            // reject all WRITE requests that come after ANY queued request.
            for (LockRequest queued : queuedRequests) {
                if (request.compareTo(queued) > 0) {
                    if (request.type == LockType.WRITE || queued.type == LockType.WRITE) {
                        return false;
                    }
                }
            }

            // If no queued requests or no conflicting queued requests, we can 
            // accept the request if it meshes with our R/W coexistence rules.
            return !held || (mode == LockType.READ && request.type == LockType.READ);
        }
        
        public boolean acquire(LockType wantType, long sessionId) {
            lockLock.lock();
            LockRequest request = new LockRequest(wantType, sessionId,
                    logicalTime.incrementAndGet(), lockLock.newCondition());
            
            if (ownsLock(LockType.READ, sessionId) && wantType == LockType.WRITE) {
                throw new IllegalStateException("Cannot upgrade lock from READ TO WRITE for session " + sessionId);
            }

            // Requests must be made linearly -- duplicate requests => retries
            invalidateRequestsBy(sessionId);
            
            try {
                queuedRequests.add(request);
                while (request.isValid() && !shouldGrantLock(request)) {
                    request.sleep();
                }
                queuedRequests.remove(request);
                
                if (!request.isValid()) {
                    // Make sure if we're no longer valid, that we wake up other
                    // (possibly valid) queued requests.
                    if (!held) {
                        wakeNextQueuedGroup();
                    }
                    logger.info("Refusing invalidated request: " + request);
                    return false;
                }

                this.held = true;
                this.lockers.add(sessionId);
                this.mode = wantType;
                return true;
            } finally {
                lockLock.unlock();
            }
        }
        
        public void release(long sessionId) {
            lockLock.lock();
            try {
                lockers.remove(sessionId);
                if (lockers.isEmpty()) {
                    held = false;
                    wakeNextQueuedGroup();
                }
            } finally {
                lockLock.unlock();
            }
        }
        
        /** Wakes the next queued writer or consecutively queued readers. */
        private void wakeNextQueuedGroup() {
            boolean wokeReader = false;
            for (LockRequest request : queuedRequests) {
                if (request.type == LockType.READ) {
                    request.wake();
                    wokeReader = true;
                } else if (request.type == LockType.WRITE) {
                    if (!wokeReader) {
                        request.wake();
                    }
                    break;
                }
            }
        }
        
        /** Returns true if the session owns the lock at the given level or above.
         * This method thus returns true if we own a WriteLock and want to READ. */
        public boolean ownsLock(LockType needType, long sessionId) {
            lockLock.lock();
            try {
                return ownsAnyLock(sessionId) &&
                        (needType == LockType.READ || mode == LockType.WRITE);
            } finally {
                lockLock.unlock();
            }
        }
        
        public boolean ownsAnyLock(long sessionId) {
            return lockers.contains(sessionId);
        }

        public void invalidateRequestsBy(long sessionId) {
            lockLock.lock();

            try {
                for (LockRequest request : queuedRequests) {
                    if (request.sessionId == sessionId) {
                        request.invalidate();
                    }
                }
            } finally {
                lockLock.unlock();
            }
        }
    }
    
    private ConcurrentMap<String, LockState> lockTable;
    
    public TwoPLLocalLockManager() {
        lockTable = Maps.newConcurrentMap();
    }
    
    public boolean ownsLock(LockType type, String key, long sessionId) {
        if (lockTable.containsKey(key)) {
            return lockTable.get(key).ownsLock(type, sessionId);
        }
        return false;
    }
    
    /**
     * Locks the key for the given session, blocking as necessary.
     * Returns immediately if this session already has the lock.
     */
    // TODO: Consider using more performant code when logic settles down.
    // See: http://stackoverflow.com/a/13957003,
    //      and http://www.day.com/maven/jsr170/javadocs/jcr-2.0/javax/jcr/lock/LockManager.html
    public void lock(LockType lockType, String key, long sessionId) {
        
        lockTable.putIfAbsent(key, new LockState());
        LockState lockState = lockTable.get(key);
        
        if (lockState.ownsLock(lockType, sessionId)) {
            logger.debug(lockType + " Lock re-granted for [" + sessionId + "] on key '" + key + "'");
            return;
        }

        logger.debug("[" + sessionId + "] wants to acquire " + lockType + " lock on '" + key + "'");
        boolean acquired = lockState.acquire(lockType, sessionId);
        if (acquired) {
            lockMetric.inc();
            logger.debug(lockType + " Lock granted for [" + sessionId + "] on key '" + key + "'");
        } else {
            logger.error("[" + sessionId + "] " +lockType + " Lock unavailable for key '" + key + "'.");
            throw new IllegalStateException("[" + sessionId + "] Unable to acquire lock for key '" + key + "'.");
        }
    }
    
    /**
     * Returns true if there is no lock for the key after this action.
     * (i.e., it was removed or no lock existed.)
     * @throws IllegalArgumentException if we don't own the lock on the key.
     */
    public synchronized void unlock(String key, long sessionId) {
        if (lockTable.containsKey(key)) {
            LockState lockState = lockTable.get(key);
            lockState.invalidateRequestsBy(sessionId);
            if (lockState.ownsAnyLock(sessionId)) {
                lockState.release(sessionId);
                lockMetric.dec();
                logger.debug("Lock released by [" + sessionId + "] on key '" + key + "'");
            } else {
            	logger.warn("[" + sessionId + "] cannot unlock key it does not own: '" + key + "'");
//                throw new IllegalArgumentException("[" + sessionId + "] cannot unlock key it does not own: '" + key + "'");
            }
        }
    }
}
