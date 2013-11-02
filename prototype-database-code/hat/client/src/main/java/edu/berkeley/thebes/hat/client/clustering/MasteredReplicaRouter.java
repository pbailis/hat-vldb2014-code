package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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

public class MasteredReplicaRouter extends ReplicaRouter {

    private final Map<Integer, List<ServerAddress>> replicaAddressesByCluster;
    private final Map<Integer, List<ReplicaService.Client>> syncReplicasByCluster;
    private final int numClusters;
    private final int numNeighbors;

    public MasteredReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.MASTERED);
        
        this.replicaAddressesByCluster = Maps.newHashMap();
        this.syncReplicasByCluster = Maps.newHashMap();
        this.numClusters = Config.getNumClusters();
        this.numNeighbors = Config.getServersInCluster().size();
        
        for (int i = 0; i < numClusters; i ++) {
            List<ServerAddress> neighbors = Config.getServersInCluster(i+1);
            List<ReplicaService.Client> neighborClients = Lists.newArrayList();
            for (ServerAddress neighbor : neighbors) {
                neighborClients.add(ThriftUtil.getReplicaServiceSyncClient(neighbor.getIP(),
                        neighbor.getPort()));
            }
            replicaAddressesByCluster.put(i+1, neighbors);
            syncReplicasByCluster.put(i+1, neighborClients);
        }
    }

    private ReplicaService.Client getSyncReplicaByKey(String key) {
        int hash = RoutingHash.hashKey(key, numNeighbors);
        int clusterID = (hash % numClusters) + 1;
        return syncReplicasByCluster.get(clusterID).get(hash);
    }

    private ServerAddress getReplicaIPByKey(String key) {
        int hash = RoutingHash.hashKey(key, numNeighbors);
        int clusterID = RoutingHash.hashKey(key, numClusters) + 1;
        return replicaAddressesByCluster.get(clusterID).get(hash);
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        try {
            return getSyncReplicaByKey(key).put(key, value.toThrift());
        } catch (TException e) {
            throw new TException("Failed to write to " + getReplicaIPByKey(key), e);
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
