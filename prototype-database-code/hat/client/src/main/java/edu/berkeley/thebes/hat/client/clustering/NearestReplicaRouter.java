package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.RoutingMode;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class NearestReplicaRouter extends ReplicaRouter {
    
    private static Logger logger = LoggerFactory.getLogger(NearestReplicaRouter.class);

    private static final double ALPHA = .95;
    private static final double TIME_BETWEEN_CHECKS = 10000;
    private static final double WARNING_THRESHOLD = 10;
    private List<ReplicaService.Client> syncReplicas;
    
    private static ConcurrentMap<ServerAddress, Double> averageLatencyByServer = Maps.newConcurrentMap();
    
    private static AtomicLong timeSinceLastCheck;

    public NearestReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.NEAREST);
        
        List<ServerAddress> serverIPs = Config.getServersInCluster();
        
        syncReplicas = new ArrayList<ReplicaService.Client>(serverIPs.size());

        for (ServerAddress server : serverIPs) {
            syncReplicas.add(ThriftUtil.getReplicaServiceSyncClient(server.getIP(), server.getPort()));
            averageLatencyByServer.putIfAbsent(server, 0d);
            logger.trace("Connected to " + server);
        }
        
        timeSinceLastCheck = new AtomicLong();
    }
    
    private ReplicaService.Client getSyncReplicaByKey(String key) {
        return syncReplicas.get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    private ServerAddress getReplicaIPByKey(String key) {
        return Config.getServersInCluster().get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        try {
            ServerAddress serverAddress = getReplicaIPByKey(key);
            ReplicaService.Client serverClient = getSyncReplicaByKey(key); 
            long startTime = System.currentTimeMillis();
            boolean ret = serverClient.put(key, value.toThrift());
            
            long duration = System.currentTimeMillis() - startTime;
            averageLatencyByServer.put(serverAddress,
                    averageLatencyByServer.get(serverAddress)*ALPHA + duration*(1-ALPHA));
            
            checkLatencies();
            return ret;
        } catch (TException e) {
            throw new TException("Failed to write to " + getReplicaIPByKey(key), e);
        }
    }
    
    private void checkLatencies() {
        if (timeSinceLastCheck.get() == 0) {
            timeSinceLastCheck.set(System.currentTimeMillis());
        }
        
        if (System.currentTimeMillis() - timeSinceLastCheck.get() < TIME_BETWEEN_CHECKS) {
            return;
        } else {
            timeSinceLastCheck.set(System.currentTimeMillis());
        }
        
        double minLatency = -1;
        for (double latency : averageLatencyByServer.values()) {
            if (minLatency == -1 || latency < minLatency) {
                minLatency = latency;
            }
        }
        
        for (ServerAddress server : averageLatencyByServer.keySet()) {
            double latency = averageLatencyByServer.get(server);
            if (latency > WARNING_THRESHOLD * minLatency) {
                logger.warn("Server " + server + " has high avg put latency: " + latency
                        + " (min avg latency=" + minLatency + ")");
            } else if (logger.isDebugEnabled()) {
                logger.debug("Server " + server + " has avg put latency: " + latency);
            }
        }
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        try {
            return getSyncReplicaByKey(key).get(key, Version.toThrift(requiredVersion));
        } catch (TException e) {
            throw new TException("Failed to read from " + getReplicaIPByKey(key), e);
        }
    }
}
