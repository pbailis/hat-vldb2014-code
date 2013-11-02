package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.apache.thrift.TException;

public class DependencyResolverTest extends TestCase {
    private static final short CLIENT_ID = 0;
    private AtomicLong logicalClock;
    
    private DependencyResolver resolver;
    private MockPersistenceEngine persistenceEngine;
    private MockRouter router;
    
    @Override
    public void setUp() {
        persistenceEngine = new MockPersistenceEngine();
        router = new MockRouter();
        resolver = new DependencyResolver(router, persistenceEngine);
        logicalClock = new AtomicLong(0);
    }
    
    public void testBasic() throws TException {
        Version xact1 = getTransactionId();
        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect(null);
        assertPending("hello", "World!", xact1);
        
        resolver.ackTransactionPending(xact1);
        assertPending("hello", "World!", xact1);
        
        resolver.ackTransactionPending(xact1);
        assertGood("hello", "World!", xact1);
    }

    public void testWaitForAllSelf() throws TException {
        Version xact1 = getTransactionId();
        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "hello", "depKey0", "depKey1", "depKey2"));
        
        try {
            router.expect(null);
            fail();
        } catch (AssertionFailedError e) { /* ok */ }
        
        assertPending("hello", "World!", xact1);
        
        resolver.ackTransactionPending(xact1);
        assertPending("hello", "World!", xact1);

        resolver.addPendingWrite("depKey0",
                makeDataItem(xact1, "Worldz!", "hello", "depKey0", "depKey1", "depKey2"));
        router.expect(null);

        assertPending("hello", "World!", xact1);
        assertPending("depKey0", "Worldz!", xact1);
        
        resolver.ackTransactionPending(xact1);
        assertPending("hello", "World!", xact1);
        assertPending("depKey0", "Worldz!", xact1);
        
        resolver.ackTransactionPending(xact1);
        assertGood("hello", "World!", xact1);
        assertGood("depKey0", "Worldz!", xact1);

    }
    
    public void testPrematureAck() throws TException {
        Version xact1 = getTransactionId();
        
        resolver.ackTransactionPending(xact1);

        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        assertPending("hello", "World!", xact1);
        
        resolver.ackTransactionPending(xact1);
        assertGood("hello", "World!", xact1);
    }
    
    public void testPrematureAckAll() throws TException {
        Version xact1 = getTransactionId();
        
        resolver.ackTransactionPending(xact1);
        
        resolver.ackTransactionPending(xact1);

        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        assertGood("hello", "World!", xact1);
    }
    
    public void testUnrelated() throws TException {
        Version xact1 = getTransactionId();
        Version xact2 = getTransactionId();
        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect(xact2);
        resolver.addPendingWrite("other",
                makeDataItem(xact2, "value!", "depKey3", "depKey4"));
        assertPending("hello", "World!", xact1);
        assertPending("other", "value!", xact2);
        
        resolver.ackTransactionPending(xact1);
        assertPending("hello", "World!", xact1);
        assertPending("other", "value!", xact2);
        
        resolver.ackTransactionPending(xact2);
        assertPending("hello", "World!", xact1);
        assertPending("other", "value!", xact2);
        
        resolver.ackTransactionPending(xact2);
        assertPending("hello", "World!", xact1);
        assertGood("other", "value!", xact2);
        
        resolver.ackTransactionPending(xact1);
        assertGood("hello", "World!", xact1);
        assertGood("other", "value!", xact2);
    }
    
    public void testSameKey() throws TException {
        Version xact1 = getTransactionId();
        Version xact2 = getTransactionId();
        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect(xact2);
        resolver.addPendingWrite("hello",
                makeDataItem(xact2, "value!", "depKey3", "depKey4"));
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.ackTransactionPending(xact1);
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.ackTransactionPending(xact2);
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.ackTransactionPending(xact2);
        assertPending("hello", "World!", xact1);
        assertGood("hello", "value!", xact2);
        
        resolver.ackTransactionPending(xact1);
        assertGood("hello", "value!", xact1);
        assertGood("hello", "value!", xact2);
    }
    
    public void testSameKeyPrematureAcks() throws TException {
        Version xact1 = getTransactionId();
        Version xact2 = getTransactionId();
        
        resolver.ackTransactionPending(xact1);
        
        resolver.ackTransactionPending(xact2);

        router.expect(xact1);
        resolver.addPendingWrite("hello",
                makeDataItem(xact1, "World!", "depKey1", "depKey2"));
        router.expect(xact2);
        resolver.addPendingWrite("hello",
                makeDataItem(xact2, "value!", "depKey3", "depKey4"));
        assertPending("hello", "World!", xact1);
        assertPending("hello", "value!", xact2);
        
        resolver.ackTransactionPending(xact2);
        assertPending("hello", "World!", xact1);
        assertGood("hello", "value!", xact2);
        
        resolver.ackTransactionPending(xact1);
        assertGood("hello", "value!", xact1);
        assertGood("hello", "value!", xact2);
    }
    
    private void assertPending(String key, String value, Version version) throws TException {
        if (resolver.retrievePendingItem(key, version) != null) {
            assertEquals(ByteBuffer.wrap(value.getBytes()),
                    resolver.retrievePendingItem(key, version).getData());
        } else {
            fail("Not in pending!");
        }
        
        if (persistenceEngine.get(key) != null) {
            assertFalse(ByteBuffer.wrap(value.getBytes()).equals(persistenceEngine.get(key).getData()));
        }
    }
    
    private void assertGood(String key, String value, Version version) {
        //assertNull( resolver.retrievePendingItem(key, version));
        assertEquals(ByteBuffer.wrap(value.getBytes()), persistenceEngine.get(key).getData());
    }
    
    private DataItem makeDataItem(Version xact, String value, String ... transactionKeys) {
        DataItem di = new DataItem(ByteBuffer.wrap(value.getBytes()), xact);
        di.setTransactionKeys(Lists.newArrayList(transactionKeys));
        return di;
    }
    
    private Version getTransactionId() {
        return new Version(CLIENT_ID, logicalClock.incrementAndGet(),
                System.currentTimeMillis());
    }
    
    
    private static class MockRouter extends AntiEntropyServiceRouter {
        private Version expectAnnounceID;
        public void expect(Version key) {
            assertNull(expectAnnounceID);
            expectAnnounceID = key;
        }
        
        @Override
        public void sendWriteToSiblings(String key, ThriftDataItem value) {

        }
        
       @Override
        public void announceTransactionReady(Version transactionID,
                Set<Integer> servers) {
           assertEquals(transactionID, expectAnnounceID);
           expectAnnounceID = null;
        }
    }
    
    private static class MockPersistenceEngine implements IPersistenceEngine {
        private final Map<String, DataItem> data = Maps.newHashMap();
        @Override
        public void force_put(String key, DataItem value) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void put_if_newer(String key, DataItem value) {
            if (!data.containsKey(key)) {
                data.put(key, value);
            } else if (data.get(key).getVersion().compareTo(value.getVersion()) <= 0) {
                data.put(key, value);
            }
            
        }

        @Override
        public DataItem get(String key) {
            return data.get(key);
        }

        @Override
        public void open() {}
        @Override
        public void close() {}

        @Override
        public void delete(String key) {}
    }
}
