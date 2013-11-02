package edu.berkeley.thebes.twopl.client;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.ConfigurationException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.TwoPLMasterRouter;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;

/** 
 * This client forwards transactions to an appropriate {@link ThebesTwoPLTransactionManager}.
 * 
 * We buffer the transaction and sends it off at the END. 
 * Accordingly, GET and PUT cannot return valid values.
 */
public class ThebesTwoPLClient implements IThebesClient {
    private final Meter requestMetric = Metrics.newMeter(ThebesTwoPLClient.class, "2pl-requests", "requests", TimeUnit.SECONDS);
    private final Meter operationMetric = Metrics.newMeter(ThebesTwoPLClient.class, "2pl-operations", "operations", TimeUnit.SECONDS);
    private final Meter errorMetric = Metrics.newMeter(ThebesTwoPLClient.class, "2pl-errors", "errors", TimeUnit.SECONDS);
    private final Timer latencyMetric = Metrics.newTimer(ThebesTwoPLClient.class, "2pl-latencies", TimeUnit.MILLISECONDS, TimeUnit.SECONDS); 

    private boolean inTransaction;
    private List<String> xactCommands;
    private TwoPLMasterRouter masterRouter;
    // Note: only ConcurrentMap for method putIfAbsent.
    private ConcurrentMap<Integer, AtomicInteger> clusterToAccessesMap;
    
    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        masterRouter = new TwoPLMasterRouter();
    }

    @Override
    public void beginTransaction() throws TException {
        if (inTransaction) {
            throw new TException("Currently in a transaction.");
        }
        clusterToAccessesMap = Maps.newConcurrentMap();
        xactCommands = Lists.newArrayList();
        inTransaction = true;
    }

    @Override
    public void abortTransaction() throws TException {
        throw new TException("abort not supported by ThebesTwoPLClient");
    }

    @Override
    public boolean commitTransaction() throws TException {
        if (!inTransaction) {
            return false;
        }
        
        requestMetric.mark();
        operationMetric.mark(xactCommands.size());
        
        // Open the transaction client with the TM that's closest to the most-used masters.
        int max = -1;
        Integer maxClusterID = null;
        for (Entry<Integer, AtomicInteger> clusterIDCount : clusterToAccessesMap.entrySet()) {
            if (maxClusterID == null || clusterIDCount.getValue().get() > max) {
                maxClusterID = clusterIDCount.getKey();
                max = clusterIDCount.getValue().get();
            }
        }
        
        if (maxClusterID == null) {
            maxClusterID = Config.getClusterID();
        }
        ServerAddress bestTM = Config.getTwoPLTransactionManagerByCluster(maxClusterID); 
        TwoPLTransactionService.Client xactClient =
                TwoPLThriftUtil.getTransactionServiceClient(bestTM.getIP(), bestTM.getPort());
        
        inTransaction = false;
        TwoPLTransactionResult result;
        final TimerContext timer = latencyMetric.time();
        try {
            result = xactClient.execute(xactCommands);
            timer.stop();
            System.out.println("Transaction committed successfully.");
            
            for (Entry<String, ByteBuffer> value : result.requestedValues.entrySet()) {
                System.out.println("Returned: " + value.getKey() + " -> "
                        + value.getValue().getInt());
            }
            return true;
        } catch (TTransactionAbortedException e) {
            System.out.println("ERROR: " + e.getErrorMessage());
            System.out.println("Transaction aborted.");
            errorMetric.mark();
            return false;
        } catch (RuntimeException e) {
            errorMetric.mark();
            throw e;
        } finally {
            timer.stop();
        }
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add("put " + key + " " + new String(value.array()));
        
        incrementCluster(key);
        return true;
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add("get " + key);
        incrementCluster(key);
        return null;
    }
    
    private void incrementCluster(String key) {
        ServerAddress address = masterRouter.getMasterAddressByKey(key);
        int clusterID = address.getClusterID();
        clusterToAccessesMap.putIfAbsent(clusterID, new AtomicInteger(0));
        clusterToAccessesMap.get(clusterID).incrementAndGet();
    }

    /** Adds a raw command accepted by the Thebes Transactional Language (TTL). */
    @Override
    public void sendCommand(String cmd) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add(cmd);
    }
    
    public void close() { return; }
}