package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.RoutingMode;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.AsyncClient.put_call;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.Client;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.AsyncClient.get_call;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class QuorumReplicaRouter extends ReplicaRouter {

    private static Logger logger = LoggerFactory.getLogger(QuorumReplicaRouter.class);

    private final Map<Integer, List<ServerAddress>> replicaAddressesByCluster;
    private final int numClusters;
    private final int numNeighbors;
    private final int quorum;

    private final Map<ServerAddress, ReplicaClient> replicaRequestQueues = Maps.newHashMap();

    private class ReplicaClient {
        private Client client;
        private AtomicBoolean inUse;
        BlockingQueue<Request<?>> requestBlockingQueue;

        public ReplicaClient(Client client) {
            this.client = client;
            this.inUse = new AtomicBoolean(false);
            requestBlockingQueue = Queues.newLinkedBlockingQueue();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    while(true) {
                        Request<?> request = Uninterruptibles.takeUninterruptibly(requestBlockingQueue);
                        request.process(ReplicaClient.this);
                    }
                }
            }).start();
        }

        public boolean executeRequest(Request<?> request) {
            if(!inUse.getAndSet(true)) {
                requestBlockingQueue.add(request);
                return true;
            }
            return false;
        }
    }

    public QuorumReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.QUORUM);

        this.replicaAddressesByCluster = Maps.newHashMap();
        this.numClusters = Config.getNumClusters();
        this.numNeighbors = Config.getServersInCluster().size();
        this.quorum = (int) Math.ceil((numNeighbors+1)/2);

        assert(this.quorum <= this.numNeighbors);

        logger.debug("quorum is set to "+this.quorum);

        for (int i = 0; i < numClusters; i ++) {
            List<ServerAddress> neighbors = Config.getServersInCluster(i+1);
            for (ServerAddress neighbor : neighbors) {
                logger.debug("Connecting to " + neighbor);
                replicaRequestQueues.put(neighbor, new ReplicaClient(
                        ThriftUtil.getReplicaServiceSyncClient(neighbor.getIP(), neighbor.getPort())));
            }
            replicaAddressesByCluster.put(i+1, neighbors);
        }
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        return performRequest(key, new WriteRequest(key, value));
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        return performRequest(key, new ReadRequest(key, requiredVersion));
    }

    /** Performs the request by queueing N requests and waiting for Q responses. */
    public <E> E performRequest(String key, Request<E> request) {
        int numSent = 0;
        int numAttempted = 0;
        int replicaIndex = RoutingHash.hashKey(key, numNeighbors);
        for (List<ServerAddress> replicasInCluster : replicaAddressesByCluster.values()) {
            ServerAddress replicaAddress = replicasInCluster.get(replicaIndex);
            ReplicaClient replica = replicaRequestQueues.get(replicaAddress);
            numAttempted++;
            if(replica.executeRequest(request))
                numSent++;
        }

        assert numSent >= quorum;

        if(numSent < quorum)
            logger.warn(String.format("attempted %d, sent %d, need %d", numAttempted, numSent, quorum));

        logger.trace("Waiting for response");
        E ret = request.getResponseWhenReady();
        logger.trace("Got response");
        return ret;
    }

    private abstract class Request<E> {
        private BlockingQueue<E> responseChannel;
        private Semaphore responseSemaphore;
        AtomicInteger numResponses;

        private Request() {
            this.responseChannel = Queues.newLinkedBlockingQueue();
            responseSemaphore = new Semaphore(0);
            numResponses = new AtomicInteger(0);
        }

        abstract public void process(ReplicaClient client);

        protected void notifyResponse(E response) {
            responseChannel.add(response);
            responseSemaphore.release();
        }

        public E getResponseWhenReady() {
            responseSemaphore.acquireUninterruptibly(quorum);
            return Uninterruptibles.takeUninterruptibly(responseChannel);
        }
    }

    private class WriteRequest extends Request<Boolean> {
        private String key;
        private ThriftDataItem value;

        public WriteRequest(String key, DataItem value) {
            this.key = key;
            this.value = value.toThrift();
        }

        public void process(ReplicaClient replica) {
            try {
                replica.client.put(key, value);
                replica.inUse.set(false);
                notifyResponse(true);
            } catch (TException e) {
                logger.error("Error: ", e);
                replica.inUse.set(false);
                notifyResponse(false);
            }
        }
    }

    private class ReadRequest extends Request<ThriftDataItem> {
        private String key;
        private Version requiredVersion;

        public ReadRequest(String key, Version requiredVersion) {
            this.key = key;
            this.requiredVersion = requiredVersion != null ? requiredVersion : Version.NULL_VERSION;
        }

        public void process(ReplicaClient replica) {
            try {
                ThriftDataItem resp = replica.client.get(key, requiredVersion.getThriftVersion());

                replica.inUse.set(false);
                notifyResponse(resp);
            } catch (TException e) {
                logger.error("Exception:", e);

                if (numResponses.incrementAndGet() >= quorum) {
                    replica.inUse.set(false);
                    notifyResponse(new ThriftDataItem()); // "null"
                }
            }
        }
    }
}
