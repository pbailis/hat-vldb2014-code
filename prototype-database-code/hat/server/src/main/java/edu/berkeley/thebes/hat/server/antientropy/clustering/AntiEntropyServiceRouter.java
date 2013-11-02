package edu.berkeley.thebes.hat.server.antientropy.clustering;


import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import edu.berkeley.thebes.hat.server.dependencies.PendingWrite;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class AntiEntropyServiceRouter {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceRouter.class);

    Meter writeForwardCount = Metrics.newMeter(AntiEntropyServiceRouter.class,
                                               "write-forward-events",
                                               "events",
                                               TimeUnit.SECONDS);

    Meter announceWriteCount = Metrics.newMeter(AntiEntropyServiceRouter.class,
                                                "write-announce-events",
                                                "events",
                                                TimeUnit.SECONDS);

    Histogram aeBatchSize = Metrics.newHistogram(AntiEntropyServiceRouter.class,
                                                 "anti-entropy-batch-size");

    Histogram taBatchSize = Metrics.newHistogram(AntiEntropyServiceRouter.class,
                                                 "ta-batch-size");

    Histogram taUncompressedSize = Metrics.newHistogram(AntiEntropyServiceRouter.class,
                                                     "ta-uncompressed-batch-bytes");

    Histogram taCompressedSize = Metrics.newHistogram(AntiEntropyServiceRouter.class,
                                                     "ta-compressed-batch-bytes");



    public void bootstrapAntiEntropyRouting() throws TTransportException {
        if (Config.isStandaloneServer()) {
            logger.debug("Server marked as standalone; not starting anti-entropy (jk)!");
            // TODO: Fix this.
//            return;
        }

        Uninterruptibles.sleepUninterruptibly(Config.getAntiEntropyBootstrapTime(),
                TimeUnit.MILLISECONDS);

        logger.debug("Bootstrapping anti-entropy...");

        logger.trace("Starting thread to forward writes to siblings...");
        for (int i = 0; i < Config.getNumAntiEntropyThreads(); i ++) {
            Thread t = new Thread() {
                public void run() {
                    List<AntiEntropyService.Client> replicaSiblingClients =
                            createClientsFromAddresses(Config.getSiblingServers());
                    while (true) {
                        writeForwardCount.mark();

                        forwardNextQueuedWriteToSiblings(replicaSiblingClients);
                    }
                }
            };
            t.setPriority(Thread.NORM_PRIORITY-2);
            t.start();
        }

        logger.trace("Starting thread to announce new pending writes...");
        for (int i = 0; i < Config.getNumTAAntiEntropyThreads(); i ++) {
            Thread t = new Thread() {
                public void run() {
                    List<AntiEntropyService.Client> neighborClients =
                            createClientsFromAddresses(Config.getServersInCluster());
                    while (true) {
                        announceWriteCount.mark();

                        announceNextQueuedPendingWrite(neighborClients);
                    }
                }
            };
            t.setPriority(Thread.NORM_PRIORITY-3);
            t.start();
        }

        logger.debug("...anti-entropy bootstrapped");

    }

    /** Stores the writes we receive and need to forward to all siblings */
    private final LinkedBlockingQueue<QueuedWrite> writesToForwardSiblings;
    /** Stores the writes we've put into pending, and need to notify all dependent neighbors. */
    private final LinkedBlockingQueue<QueuedTransactionAnnouncement> pendingTransactionAnnouncements;

    public AntiEntropyServiceRouter() {
        this.writesToForwardSiblings = Queues.newLinkedBlockingQueue();
        this.pendingTransactionAnnouncements = Queues.newLinkedBlockingQueue();
    }
    
    /** Our cluster got a new write, forward to the replicas in other clusters. */
    public void sendWriteToSiblings(String key, ThriftDataItem value) {
        writesToForwardSiblings.add(new QueuedWrite(key, value));
    }

    /** Actually does the forwarding! Called in its own thread. */
    private void forwardNextQueuedWriteToSiblings(List<AntiEntropyService.Client> siblings) {
        ServerAddress tryServer = null;
        try {
            List<QueuedWrite> writes = Lists.newArrayList();
            writes.add(writesToForwardSiblings.take());
            Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
            writesToForwardSiblings.drainTo(writes);

            List<String> keys = Lists.newArrayListWithExpectedSize(writes.size());
            List<ThriftDataItem> values = Lists.newArrayListWithExpectedSize(writes.size());
            
            for (QueuedWrite write : writes) {
                keys.add(write.key);
                values.add(write.value);
            }
            
            aeBatchSize.update(writes.size());
            
            int i = 0;
            for (AntiEntropyService.Client sibling : siblings) {
                tryServer = Config.getSiblingServers().get(i++);
                sibling.put(keys, values);
            }
        } catch (TException e) {
            logger.error("Failure while forwarding write to siblings (" + tryServer + "): ", e);
        } catch (InterruptedException e) {
            logger.error("Interrupted: ", e);
        }
    }

    /** Announce that a transaction is ready to some set of servers. */
    public void announceTransactionReady(Version transactionID, Set<Integer> servers) {
        pendingTransactionAnnouncements.add(
                new QueuedTransactionAnnouncement(transactionID, servers));
    }
    
    /** Actually does the announcement! Called in its own thread. */
    private void announceNextQueuedPendingWrite(List<AntiEntropyService.Client> neighbors) {
        ServerAddress tryServer = null;

        try {
            List<QueuedTransactionAnnouncement> announcements = Lists.newArrayList();
            announcements.add(pendingTransactionAnnouncements.take());
            Uninterruptibles.sleepUninterruptibly(Config.getTABatchTime(), TimeUnit.MILLISECONDS);
            pendingTransactionAnnouncements.drainTo(announcements);
            
            Map<Integer, List<Long>> versionByServer = Maps.newHashMap();
            
            int numSending = 0;
            for (QueuedTransactionAnnouncement ann : announcements) {
                for (Integer serverIndex : ann.servers) {
                    if (!versionByServer.containsKey(serverIndex)) {
                        versionByServer.put(serverIndex, new ArrayList<Long>());
                    }
                    numSending ++;
                    versionByServer.get(serverIndex).add(ann.transactionID.getThriftVersion().getVersion());
                }
            }
            taBatchSize.update(numSending);
            for (Integer serverIndex : versionByServer.keySet()) {
                AntiEntropyService.Client neighborClient = neighbors.get(serverIndex);

                List<Long> versionsToSend = versionByServer.get(serverIndex);

                ByteArrayOutputStream baos = new ByteArrayOutputStream(versionsToSend.size()*Long.SIZE);
                DataOutputStream dos = new DataOutputStream(baos);

                for(long toSend : versionByServer.get(serverIndex)) {
                    dos.writeLong(toSend);
                }

                byte[] uncompressedIds = baos.toByteArray();
                byte[] compressedIds = Snappy.compress(uncompressedIds);

                taUncompressedSize.update(uncompressedIds.length);
                taCompressedSize.update(compressedIds.length);

                neighborClient.ackTransactionPending(ByteBuffer.wrap(compressedIds));
            }
        } catch (IOException e) {
            logger.error("Failure while serializing ", e);
        } catch (TException e) {
            logger.error("Failure while announcing dpending write to " + tryServer + ": ", e);
        } catch (InterruptedException e) {
                    logger.error("Interrupted: ", e);
        }
    }
    
    private List<AntiEntropyService.Client> createClientsFromAddresses(
            List<ServerAddress> addresses) {
        
        List<AntiEntropyService.Client> clients = Lists.newArrayList(); 
        for (ServerAddress address : addresses) {
            while (true) {
                try {
                    clients.add(ThriftUtil.getAntiEntropyServiceClient(
                            address.getIP(), Config.getAntiEntropyServerPort()));
                    break;
                } catch (Exception e) {
                    logger.error("Exception while bootstrapping connection with cluster server: " +
                                 address);
                    e.printStackTrace();
                }
            }
        }
        return clients;
    }
    
    private static class QueuedWrite {
        public final String key;
        public final ThriftDataItem value;
        public QueuedWrite(String key, ThriftDataItem value) {
            this.key = key;
            this.value = value;
        }
    }
    
    private static class QueuedTransactionAnnouncement {
        public final Version transactionID;
        public final Set<Integer> servers;
        public QueuedTransactionAnnouncement(Version transactionID, Set<Integer> servers) {
            this.transactionID = transactionID;
            this.servers = servers;
        }
    }
}