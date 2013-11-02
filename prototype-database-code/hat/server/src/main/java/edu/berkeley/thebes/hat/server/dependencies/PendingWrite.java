package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class PendingWrite implements Comparable<PendingWrite> {
    private final String key;
    private final Version transactionVersion;
    
    private int numKeysForThisReplica;
    private Set<Integer> replicaIndicesInvolved;
    private List<String> transactionKeys;
    private boolean obsoleted;
    
    public PendingWrite(String key, DataItem value, boolean obsoleted) {
        if (value.getTransactionKeys().isEmpty()) {
            throw new IllegalStateException("Pending writes must be waiting on at least one other key.");
        }
        
        this.key = key;
        this.transactionVersion = value.getVersion();
        this.transactionKeys = value.getTransactionKeys();
        this.obsoleted = obsoleted;

        examineReplicasInvolved(key, value.getTransactionKeys());
    }
    
    /** Computes numKeysForThisReplica and replicaIndicesInvolved */
    private void examineReplicasInvolved(String key, List<String> transactionKeys) {
        int numServers = Config.getServersInCluster().size(); 
        Set<Integer> serversInTransaction = Sets.newHashSet();
        int myIndex = RoutingHash.hashKey(key, numServers);
        this.numKeysForThisReplica = 0;
        for (String transKey : transactionKeys) {
            int i = RoutingHash.hashKey(transKey, numServers);
            serversInTransaction.add(i);
            if (i == myIndex) {
                numKeysForThisReplica ++;
            }
        }
        if (!serversInTransaction.contains(myIndex)) {
            System.err.println("WARNINGG: " + serversInTransaction + " does not include me! " + myIndex + " (" + key + " / " + transactionKeys + ")");
        }
        replicaIndicesInvolved = serversInTransaction;
    }

    public Set<Integer> getReplicaIndicesInvolved() {
        return replicaIndicesInvolved;
    }

    public int getNumKeysForThisReplica() {
        return numKeysForThisReplica;
    }

    public String getKey() {
        return key;
    }

    public Version getVersion() {
        return transactionVersion;
    }
    
    public boolean isObsoleted() {
        return obsoleted;
    }

    @Override
    public int compareTo(PendingWrite o) {
        return ComparisonChain.start()
                .compare(getKey(), o.getKey())
                .compare(getVersion(),  o.getVersion())
                .result();
    }
    
    public String toString() {
        return String.format("[Key: %s, Version: %s, Deps: %s / LocalKeys: %d, Replicas: %d", 
                key, transactionVersion, transactionKeys, numKeysForThisReplica, replicaIndicesInvolved.size());
    }
}