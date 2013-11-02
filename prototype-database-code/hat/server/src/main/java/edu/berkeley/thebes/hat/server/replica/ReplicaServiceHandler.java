package edu.berkeley.thebes.hat.server.replica;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private AntiEntropyServiceRouter antiEntropyRouter;
    private DependencyResolver dependencyResolver;
    private static Logger logger = LoggerFactory.getLogger(ReplicaServiceHandler.class);

    Meter putMeter = Metrics.newMeter(ReplicaServiceHandler.class,
                                      "put-requests",
                                      "requests",
                                      TimeUnit.SECONDS);

    Meter getMeter = Metrics.newMeter(ReplicaServiceHandler.class,
                                      "get-requests",
                                      "requests",
                                      TimeUnit.SECONDS);

    Meter nullVersionsMeter = Metrics.newMeter(ReplicaServiceHandler.class, "num-null-get-versions",
                                               "requests", TimeUnit.SECONDS);

    Meter depResRequestsMeter = Metrics.newMeter(ReplicaServiceHandler.class, "dep-res-requests",
                                               "requests", TimeUnit.SECONDS);

    private final Timer putTimer = Metrics.newTimer(ReplicaServiceHandler.class, "put-latency");
    private final Timer getTimer = Metrics.newTimer(ReplicaServiceHandler.class, "get-latency");

    public ReplicaServiceHandler(IPersistenceEngine persistenceEngine,
                                 AntiEntropyServiceRouter antiEntropyRouter,
                                 DependencyResolver dependencyResolver) {
        this.persistenceEngine = persistenceEngine;
        this.antiEntropyRouter = antiEntropyRouter;
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public boolean put(String key,
                       ThriftDataItem valueThrift) throws TException {
        TimerContext context = putTimer.time();
        try {
            DataItem value = new DataItem(valueThrift);
            if(logger.isTraceEnabled())
                logger.trace("received PUT request for key: '"+key+
                             "' value: '"+value+
                             "' transactionKeys: "+value.getTransactionKeys());
            
            antiEntropyRouter.sendWriteToSiblings(key, valueThrift);
    
            // TODO: Hmm, if siblings included us, we wouldn't even need to do this...

//            persistenceEngine.put_if_newer(key, value);

            if (value.getTransactionKeys() == null || value.getTransactionKeys().isEmpty()) {
                persistenceEngine.put_if_newer(key, value);
            } else {
                dependencyResolver.addPendingWrite(key, value);
            }
    
            putMeter.mark();
    
            // todo: remove this return value--it's really not necessary
        } finally {
            context.stop();
        }
        return true;
    }

    @Override
    public ThriftDataItem get(String key, ThriftVersion requiredVersionThrift) throws TException {
        TimerContext context = getTimer.time();
        try {
            DataItem ret = persistenceEngine.get(key);
            Version requiredVersion = Version.fromThrift(requiredVersionThrift);
            
            if(logger.isTraceEnabled())
                logger.trace("received GET request for key: '"+key+
                             "' requiredVersion: "+ requiredVersion+
                             ", found version: " + (ret == null ? null : ret.getVersion()));
    
            if (ret == null || ret.getVersion().compareTo(Version.NULL_VERSION) == 0) {
                logger.debug("Could not find key " + key);
                nullVersionsMeter.mark();
            }
            if (requiredVersion != null && requiredVersion.compareTo(Version.NULL_VERSION) != 0 &&
                    (ret == null || requiredVersion.compareTo(ret.getVersion()) > 0)) {
                depResRequestsMeter.mark();
                ret = dependencyResolver.retrievePendingItem(key, requiredVersion);
    
                // race?
                if(ret == null) {
                    logger.warn(String.format("Didn't find suitable version (timestamp=%d) for key %s in pending or persistenceEngine, so fetching again", requiredVersion.getTimestamp(), key));
                    ret = persistenceEngine.get(key);
                }
    
                if(ret == null || requiredVersion.compareTo(ret.getVersion()) > 0) {
                    logger.error(String.format("suitable version was not found! required time: %d clientID: %d only got %s",
                                                       requiredVersion.getTimestamp(), requiredVersion.getClientID(),
                                                       ret == null ? "null" : Long.toString(ret.getVersion().getTimestamp())));
                    ret = null;
                }
            }
    
            if(ret == null) {
                return new ThriftDataItem().setVersion(Version.toThrift(Version.NULL_VERSION));
            }
    
            getMeter.mark();
    
            return ret.toThrift();
        } finally {
            context.stop();
        }
    }
}