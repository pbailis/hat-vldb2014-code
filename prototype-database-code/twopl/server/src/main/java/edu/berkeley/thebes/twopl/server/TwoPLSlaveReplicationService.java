package edu.berkeley.thebes.twopl.server;

import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLSlaveReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;

public class TwoPLSlaveReplicationService {
    private static Logger logger = LoggerFactory.getLogger(TwoPLSlaveReplicationService.class);

    private List<TwoPLSlaveReplicaService.Client> slaveReplicas;
    
    public TwoPLSlaveReplicationService() {
        this.slaveReplicas = Collections.emptyList();
    }

    public void connectSlaves() {
        slaveReplicas = Lists.newArrayList();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
        }

        logger.debug("Bootstrapping slave replication service...");

        for (ServerAddress slave : Config.getSiblingServers()) {
            while (true) {
                try {
                    slaveReplicas.add(
                            TwoPLThriftUtil.getSlaveReplicaServiceClient(slave.getIP(),
                                    slave.getPort()));
                    break;
                } catch (TTransportException e) {
                    System.err.println("Exception while bootstrapping connection with slave: " +
                                       slave);
                    e.printStackTrace();
                }
            }
        }

        logger.debug("...slave replication service bootstrapped");
    }

    // TODO: race condition between serving and when we've connected to neighbors
    public void sendToSlaves(String key, ThriftDataItem value) throws TException {
        for (TwoPLSlaveReplicaService.Client slave : slaveReplicas) {
            slave.put(key, value);
        }
    }
}
