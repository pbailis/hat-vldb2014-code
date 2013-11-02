package edu.berkeley.thebes.hat.server;

import edu.berkeley.thebes.common.persistence.disk.BDBPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.LevelDBPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.WriteAheadLogger;

import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.TransactionMode;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServer;
import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServiceHandler;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler;

public class ThebesHATServer {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesHATServer.class);

    public static void main(String[] args) {
        try {
            Config.initializeServer(TransactionMode.HAT);
            Log4JConfig.configureLog4J();

            IPersistenceEngine engine = Config.getPersistenceEngine();
            engine.open();

            AntiEntropyServiceRouter router = new AntiEntropyServiceRouter();

            DependencyResolver dependencyResolver = new DependencyResolver(router, engine);

            AntiEntropyServiceHandler antiEntropyServiceHandler =
                    new AntiEntropyServiceHandler(router, dependencyResolver, engine);
            AntiEntropyServer antiEntropyServer = new AntiEntropyServer(antiEntropyServiceHandler);

            if (!Config.isStandaloneServer()) {
                (new Thread(antiEntropyServer)).start();
            } else {
                logger.debug("Server marked as standalone; not starting anti-entropy!");
            }

            router.bootstrapAntiEntropyRouting();

            ReplicaServiceHandler replicaServiceHandler = new ReplicaServiceHandler(engine,
                    router, dependencyResolver);

            logger.debug("Starting the server...");
            ThriftServer.startInCurrentThread(
                    new ReplicaService.Processor<ReplicaServiceHandler>(replicaServiceHandler),
                    Config.getServerBindIP());

        } catch (Exception e) {
            logger.error("ERROR: ", e);
            e.printStackTrace();
        }
    }
}