package edu.berkeley.thebes.hat.server.dependencies;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.WALBackedPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.WriteAheadLogger;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
The following class does most of the heavy lifting for the HAT
partial ordering (causality and transactional atomicity). The
basic idea is that each write's causal dependencies should be
present in the cluster before it is applied locally, and each
transaction's sibling dependencies should be present in
PendingWrites on their respective nodes before the write is
applied.

A transactional atomicity dependency is satisfied when there is a
write to the appropriate key in the local persistence engine with
a timestamp greater than or equal to that of the causal
dependency *or* a write in PendingWrites with an *exact* match
for a timestamp.

To check a local server in a cluster for these dependencies, a
server calls AntiEntropyService.waitForTransactionalDependency. This in turn calls
DependencyResolver.waitInQueueForDependency(). A thread in
waitInQueueForDependency queries the local persistence engine and/or
the PendingWrites list as necessary, waiting until an appropriate
dependency is found.  waitForCausalDependency() returns when the
write is in the persistence engine, while
waitForTransactionalDependency() returns when the write is (at
least) in PendingWrites.

Every time a new local write is applied, calls to
waitInQueueForDependency() need to be notified and so
notifyNewLocalWrite() is called.
*/

public class DependencyResolver {
    private static Logger logger = LoggerFactory.getLogger(DependencyResolver.class);
    
    private final AntiEntropyServiceRouter router;
    private final IPersistenceEngine persistenceEngine;
    private final IPersistenceEngine pendingPersistenceEngine;
    private final ConcurrentMap<Version, TransactionQueue> pendingTransactionsMap;
    private final ConcurrentMap<Version, AtomicInteger> unresolvedAcksMap;
    
    Meter commitCount = Metrics.newMeter(DependencyResolver.class,
                                         "dgood-transaction-total",
                                         "transactions",
                                         TimeUnit.SECONDS);

    Timer retrievePendingTimer = Metrics.newTimer(DependencyResolver.class,
                                             "dpending-retrieval");
    
    Meter weirdErrorCount = Metrics.newMeter(DependencyResolver.class,
                                             "assertion-violation",
                                             "violations",
                                             TimeUnit.SECONDS);
    Meter obsoletePuts = Metrics.newMeter(DependencyResolver.class,
                                          "obsolete-puts",
                                          "puts",
                                          TimeUnit.SECONDS);

    private final Timer addPendingTimer = Metrics.newTimer(DependencyResolver.class, "add-pending-latency");
    

    private final Timer stage1Timer = Metrics.newTimer(DependencyResolver.class, "stage-1");
    private final Timer stage15Timer = Metrics.newTimer(DependencyResolver.class, "stage-1.5");
    private final Timer stage2Timer = Metrics.newTimer(DependencyResolver.class, "stage-2");
    private final Timer stage3Timer = Metrics.newTimer(DependencyResolver.class, "stage-3");
    private final Timer stage4Timer = Metrics.newTimer(DependencyResolver.class, "stage-4");
    private final Timer stage5Timer = Metrics.newTimer(DependencyResolver.class, "stage-5");
    private final Timer stage6Timer = Metrics.newTimer(DependencyResolver.class, "stage-6");

    public DependencyResolver(AntiEntropyServiceRouter router,
            IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
        this.router = router;
        this.pendingTransactionsMap = Maps.newConcurrentMap();
        this.unresolvedAcksMap = Maps.newConcurrentMap();
        
        if (Config.shouldStorePendingInMemory()) {
            pendingPersistenceEngine = new MemoryPersistenceEngine();
        } else {
            pendingPersistenceEngine = new WALBackedPersistenceEngine(Config.getPendingWritesDB());
//            pendingPersistenceEngine = new LevelDBPersistenceEngine(Config.getPendingWritesDB());
        }
        try {
            pendingPersistenceEngine.open();
        } catch (IOException e) {
            logger.error("Failed to make pending PersistenceEngine: ", e);
        }


        Metrics.newGauge(DependencyResolver.class, "num-pending-versions", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return pendingTransactionsMap.size();
            }
        });

        Metrics.newGauge(DependencyResolver.class, "num-unresolved-versions", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return unresolvedAcksMap.size();
            }
        });
    }

    private String getPendingKeyForVersion(String key, Version version) {
        return new StringBuilder().append("VERSION").append(key).append(version).toString();
    }

    private String getPendingKeyForValue(String key, DataItem value) {
        return getPendingKeyForVersion(key, value.getVersion());
    }

    private void persistPendingWrite(String key, DataItem value) throws TException {
        pendingPersistenceEngine.force_put(getPendingKeyForValue(key, value), value);
    }

    private DataItem getPendingWrite(String key, Version version) throws TException {
        DataItem d = pendingPersistenceEngine.get(getPendingKeyForVersion(key, version));
        if (d == null || d.getVersion() == Version.NULL_VERSION || d.getVersion() == null) {
            logger.error("Returning NULL data for key=" + key + ", version=" + version);
        }
        return d;
    }

    private void deletePendingWrite(String key, Version version) throws TException {
        pendingPersistenceEngine.delete(getPendingKeyForVersion(key, version));
    }

    public void addPendingWrite(String key, DataItem value) throws TException {
        TimerContext context = addPendingTimer.time();
        try {
            Version version = value.getVersion();
            boolean obsoletedWrite = false;
            
            {
                TimerContext s1 = stage1Timer.time();
                pendingTransactionsMap.putIfAbsent(version, new TransactionQueue(version));
                s1.stop();
            }
            
            {
                TimerContext s15 = stage15Timer.time();
                DataItem curItem = persistenceEngine.get(key);
                if (curItem != null && curItem.getVersion() != null && 
                        curItem.getVersion().compareTo(version) > 0) {
                    obsoletePuts.count();
                    obsoletedWrite = true;
                }
                s15.stop();
            }
            
            if (!obsoletedWrite) {
                TimerContext s2 = stage2Timer.time();
                
                persistPendingWrite(key, value);
                s2.stop();
            }

            PendingWrite newPendingWrite;
            {
                TimerContext s3 = stage3Timer.time();
                newPendingWrite = new PendingWrite(key, value, obsoletedWrite);
                s3.stop();
            }

            TransactionQueue transQueue;
            {
                TimerContext s4 = stage4Timer.time();
                transQueue = pendingTransactionsMap.get(version);
                if (transQueue == null) {
                    weirdErrorCount.mark();
                    String message = "XACT NULL ERROR. ";
                    logger.error(message);
                    return;
                }
                s4.stop();
            }
    
            {
                TimerContext s5 = stage5Timer.time();
                try {
                    transQueue.add(newPendingWrite);
                } catch (Exception e) {
                    logger.error("Error on version " + version + ": ", e);
                }
                s5.stop();
            }
    
            {
                TimerContext s6 = stage6Timer.time();
                if (transQueue.shouldAnnounceTransactionReady()) {
                    router.announceTransactionReady(version, transQueue.replicaIndicesInvolved);
                }
                s6.stop();
            }
        } finally {
            context.stop();
        }
        // TODO: if it's still there after a while, can resend

        // Check any unresolved acks associated with this key
        // TODO: Examine the implications of this!
//        ackUnresolved(transQueue, version);
//
//        if (transQueue.canCommit()) {
//            logger.debug("Committing via unresolved: " + version + " / " + transQueue.numReplicasInvolved + " / " + newPendingWrite.getReplicaIndicesInvolved().size());
//            commit(transQueue);
//        }
    }
    
    private void commit(TransactionQueue queue) throws TException {
        for (PendingWrite write : queue.pendingWrites) {
            if (!write.isObsoleted()) {
                persistenceEngine.put_if_newer(write.getKey(), getPendingWrite(write.getKey(), write.getVersion()));
            }
            deletePendingWrite(write.getKey(), write.getVersion());
        }
//        TransactionQueue prevQueue = tempMap.put(queue.version, queue);
//        if (prevQueue != null) {
//            logger.error("Tried to commit twice for same version (" + queue.version + ") ???: " + prevQueue);
//        }
        commitCount.mark();

        // Remove all state re: this version
        pendingTransactionsMap.remove(queue.version);
        unresolvedAcksMap.remove(queue.version);
    }

    public DataItem retrievePendingItem(String key, Version version) throws TException {

        TimerContext context = retrievePendingTimer.time();

        try {
            if (!pendingTransactionsMap.containsKey(version)) {
                return null;
            }
            
            for (PendingWrite pendingWrite : pendingTransactionsMap.get(version).pendingWrites) {
                if (key.equals(pendingWrite.getKey())) {
                    return getPendingWrite(pendingWrite.getKey(), pendingWrite.getVersion());
                }
            }
            
            return null;
        } finally {
            context.stop();
        }
    }

    public void ackTransactionPending(Version transactionId) throws TException {
        TransactionQueue transactionQueue = pendingTransactionsMap.get(transactionId);
        if (transactionQueue != null) {
            ackUnresolved(transactionQueue, transactionId);
            transactionQueue.serverAcked();
            if (transactionQueue.canCommit()) {
                if (logger.isDebugEnabled()) { logger.trace("Committing via ack: " + transactionId + " / " + transactionQueue.numReplicasInvolved); }
                    commit(transactionQueue);
            }
            return;
        }
        
        // No currently known PendingWrites wanted our ack!
        // Hopefully we'll soon have one that does, so keep it around.
        unresolvedAcksMap.putIfAbsent(transactionId, new AtomicInteger(0));
        unresolvedAcksMap.get(transactionId).incrementAndGet();
        
        // Check for race conditions where the transaction arrived while we were adding this!
        transactionQueue = pendingTransactionsMap.get(transactionId);
        
        if (transactionQueue != null) {
            ackUnresolved(transactionQueue, transactionId);
        }
        
        if (transactionQueue != null && transactionQueue.canCommit()) {
            logger.debug("Committing via unresolved RACE: " + transactionId);
            commit(transactionQueue);
        }
    }
    
    /** Should own unresolvedAcksLock while calling this. */
    private void ackUnresolved(TransactionQueue transQueue, Version version) {
        if (unresolvedAcksMap.containsKey(version)) {
            AtomicInteger numAcksForTransactionAtomic = unresolvedAcksMap.get(version);
            if (numAcksForTransactionAtomic == null) {
                return;
            }
            
            int numAcksForTransaction = numAcksForTransactionAtomic.getAndSet(0);
            for (int i = 0; i < numAcksForTransaction; i ++) {
                transQueue.serverAcked();
            }
        }
    }
    
    private static class TransactionQueue {
        private final Version version;
        private int numKeysForThisReplica;
        private int numReplicasInvolved; 
        private Set<Integer> replicaIndicesInvolved;
        private final Set<PendingWrite> pendingWrites;
        private AtomicBoolean alreadySentAnnouncement = new AtomicBoolean(false);
        private AtomicBoolean alreadyCommitted = new AtomicBoolean(false);
        private AtomicInteger numReplicasAcked;
        
        public TransactionQueue(Version version) {
            this.version = version;
            this.pendingWrites = new ConcurrentSkipListSet<PendingWrite>();
            this.numReplicasAcked = new AtomicInteger(0);
        }
        
        public void add(PendingWrite write) {
            pendingWrites.add(write);
            if (!(numKeysForThisReplica == 0 ||
                    numKeysForThisReplica == write.getNumKeysForThisReplica())) {
                logger.error(String.format("numReplicasInvolved is %d, replicaIndicesInvolved is %d, key is %s, version is %s", numReplicasInvolved, write.getReplicaIndicesInvolved().size(), write.getKey(), write.getVersion()));
                assert(false);
            }
            if (!(numReplicasInvolved == 0 ||
                    numReplicasInvolved == write.getReplicaIndicesInvolved().size())) {
                logger.error(String.format("numReplicasInvolved is %d, replicaIndicesInvolved is %d, key is %s, version is %s", numReplicasInvolved, write.getReplicaIndicesInvolved().size(), write.getKey(), write.getVersion()));
                assert(false);
            }
            
            this.numKeysForThisReplica = write.getNumKeysForThisReplica();
            this.numReplicasInvolved = write.getReplicaIndicesInvolved().size();
            this.replicaIndicesInvolved = write.getReplicaIndicesInvolved();
        }
        
        public boolean canCommit() {
            return pendingWrites.size() > 0 && 
                    numReplicasAcked.get() >= numReplicasInvolved && pendingWrites.size() == numKeysForThisReplica
                    && !alreadyCommitted.getAndSet(true);
        }
        
        public void serverAcked() {
            numReplicasAcked.incrementAndGet(); 
        }
        
        public boolean shouldAnnounceTransactionReady() {
            return pendingWrites.size() == numKeysForThisReplica
                    && !alreadySentAnnouncement.getAndSet(true);
        }
        
        public String toString() {
            String ret = String.format("[LocalKeys: %d/%d (aSa?: %s), Replicas: %d/%d (aC?: %s)]",
                    pendingWrites.size(), numKeysForThisReplica, alreadySentAnnouncement.get(),
                    numReplicasAcked.get(), numReplicasInvolved, alreadyCommitted.get());
            for (PendingWrite pw : pendingWrites) {
                ret += "\n -> " + pw;
            }
            return ret;
        }
    }
}