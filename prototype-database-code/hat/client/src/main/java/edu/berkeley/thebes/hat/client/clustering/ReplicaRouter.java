package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.RoutingMode;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;

import java.io.IOException;

public abstract class ReplicaRouter {
    public static ReplicaRouter newInstance(RoutingMode routingMode)
            throws IOException, TTransportException {
        switch (routingMode) {
        case NEAREST:
            return new NearestReplicaRouter();
        case QUORUM:
            return new QuorumReplicaRouter();
        case MASTERED:
            return new MasteredReplicaRouter();
        default:
            throw new IllegalArgumentException();
        }
    }
    
    abstract public boolean put(String key, DataItem value) throws TException;
    abstract public ThriftDataItem get(String key, Version requiredVersion) throws TException;
}