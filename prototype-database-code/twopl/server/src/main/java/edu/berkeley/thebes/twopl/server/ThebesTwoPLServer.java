package edu.berkeley.thebes.twopl.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.TransactionMode;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLSlaveReplicaService;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import javax.naming.ConfigurationException;

public class ThebesTwoPLServer {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesTwoPLServer.class);

    private static final Counter masterServersMetric = Metrics.newCounter(TwoPLLocalLockManager.class, "2pl-masters");
    private static final Counter slaveServersMetric = Metrics.newCounter(TwoPLLocalLockManager.class, "2pl-slaves");

    private static TwoPLSlaveReplicationService slaveReplicationService;
    
    public static void startMasterServer(TwoPLMasterServiceHandler serviceHandler) {
        logger.debug("Starting master server...");
        masterServersMetric.inc();

        // Connect to slaves to replicate to them.
        if (Config.shouldReplicateToTwoPLSlaves()) {
            new Thread() {
                @Override
                public void run() {
                    slaveReplicationService.connectSlaves();
                }
            }.start();
        }
        
        ThriftServer.startInCurrentThread(
                new TwoPLMasterReplicaService.Processor<TwoPLMasterServiceHandler>(serviceHandler),
                Config.getTwoPLServerBindIP());
    }
    
    public static void startSlaveServer(TwoPLSlaveServiceHandler serviceHandler) {
        logger.debug("Starting slave server...");
        slaveServersMetric.inc();
        ThriftServer.startInCurrentThread(
                new TwoPLSlaveReplicaService.Processor<TwoPLSlaveServiceHandler>(serviceHandler),
                Config.getTwoPLServerBindIP());
    }

    public static void main(String[] args) {
        try {
            Config.initializeServer(TransactionMode.TWOPL);
            Log4JConfig.configureLog4J();

            IPersistenceEngine engine = Config.getPersistenceEngine();
            engine.open();

            TwoPLLocalLockManager lockManager = new TwoPLLocalLockManager();
            slaveReplicationService = new TwoPLSlaveReplicationService();
            if (Config.isMaster()) {
                startMasterServer(new TwoPLMasterServiceHandler(engine, lockManager,
                        slaveReplicationService));
            } else {
                startSlaveServer(new TwoPLSlaveServiceHandler(engine)); 
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}