package edu.berkeley.thebes.hat.server.antientropy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import edu.berkeley.thebes.hat.common.thrift.DataDependencyRequest;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import org.xerial.snappy.Snappy;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceHandler.class);
    
    DependencyResolver dependencyResolver;
    AntiEntropyServiceRouter router;
    IPersistenceEngine persistenceEngine;

    Meter putRequests = Metrics.newMeter(AntiEntropyServiceHandler.class,
                                         "put-requests",
                                         "requests",
                                         TimeUnit.SECONDS);

    Meter ackTransactionPending = Metrics.newMeter(AntiEntropyServiceHandler.class,
                                                   "ack-transaction-pending-requests",
                                                   "requests",
                                                   TimeUnit.SECONDS);


    public AntiEntropyServiceHandler(AntiEntropyServiceRouter router,
            DependencyResolver dependencyResolver, IPersistenceEngine persistenceEngine) {
        this.dependencyResolver = dependencyResolver;
        this.router = router;
        this.persistenceEngine = persistenceEngine;
    }

    @Override
    public void put(List<String> keys,
                    List<ThriftDataItem> values) throws TException{
        putRequests.mark();
        for (int i = 0; i < keys.size(); i ++) {
            String key = keys.get(i);
            DataItem value = new DataItem(values.get(i));
            logger.trace("Received anti-entropy put for key " + key);
            if (value.getTransactionKeys() == null || value.getTransactionKeys().isEmpty()) {
                persistenceEngine.put_if_newer(key, value);
            } else {
                dependencyResolver.addPendingWrite(key, value);
            }
        }
    }

    @Override
    public void ackTransactionPending(ByteBuffer transactionIdList) throws TException {
        ackTransactionPending.mark();
        
        List<Version> transactionVersions = Lists.newArrayList();

        try {
            byte[] uncompressedList = Snappy.uncompress(transactionIdList.array());

            ByteArrayInputStream bis = new ByteArrayInputStream(uncompressedList);
            DataInputStream dis = new DataInputStream(bis);

            while(bis.available() > 0) {
                transactionVersions.add(Version.fromLong(dis.readLong()));
            }

        } catch (IOException e) {
            logger.error("Error in deserialization", e);
        }

        for (Version transactionId : transactionVersions) {
            dependencyResolver.ackTransactionPending(transactionId);
        }
    }
}