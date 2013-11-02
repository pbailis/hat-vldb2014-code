package edu.berkeley.thebes.twopl.server;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.twopl.server.TwoPLLocalLockManager.LockType;
import junit.framework.AssertionFailedError;
import junit.framework.TestCase;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TwoPLLocalLockManagerTest extends TestCase {
    TwoPLLocalLockManager lockManager;
    
    private AtomicInteger owners;
    private List<AssertionFailedError> errors;
    
    static {
//        Log4JConfig.configureLog4J();
    }
    
    @Override
    public void setUp() {
        lockManager = new TwoPLLocalLockManager();
        owners = new AtomicInteger(0);
        errors = Lists.newCopyOnWriteArrayList();
    }
    
    public void testBasic() {
        lockManager.lock(LockType.READ, "abc", 123);
        assertTrue(lockManager.ownsLock(LockType.READ, "abc", 123));
        lockManager.unlock("abc", 123);
        lockManager.lock(LockType.WRITE, "qed", 123);
        assertTrue(lockManager.ownsLock(LockType.READ, "qed", 123));
        assertTrue(lockManager.ownsLock(LockType.WRITE, "qed", 123));
        lockManager.unlock("qed", 123);
    }
    
    public void testMultipleReaders() {
        
        final int NUM_THREADS = 10;
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i ++) {
            final int index = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        lockManager.lock(LockType.READ, "abc", index);
                        owners.incrementAndGet();
                        assertTrue(lockManager.ownsLock(LockType.READ, "abc", index));
                        assertFalse(lockManager.ownsLock(LockType.WRITE, "abc", index));
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { fail(); }
                        
                        assertEquals(NUM_THREADS, owners.get());
    
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { fail(); }
                        lockManager.unlock("abc", index);
                        owners.decrementAndGet();
                    } catch (AssertionFailedError e) {
                        errors.add(e);
                    }
                }
            };
            threads[i].start();
        }
        
        for (Thread t : threads) { 
            try {
                t.join();
            } catch (InterruptedException e) { fail(); }
        }
        
        for (AssertionFailedError e : errors) {
            throw e;
        }
    }
    
    public void testMultipleWriters() {
        
        final int NUM_THREADS = 5;
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i ++) {
            final int index = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        lockManager.lock(LockType.WRITE, "abc", index);
                        owners.incrementAndGet();
                        assertTrue(lockManager.ownsLock(LockType.WRITE, "abc", index));
                        assertTrue(lockManager.ownsLock(LockType.READ, "abc", index));
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { fail(); }
                        
                        assertEquals(1, owners.get());
    
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { fail(); }
                        System.out.println("Unlockly!");
                        lockManager.unlock("abc", index);
                        owners.decrementAndGet();
                    } catch (AssertionFailedError e) {
                        errors.add(e);
                    }
                }
            };
            threads[i].start();
        }
        
        for (Thread t : threads) { 
            try {
                t.join();
            } catch (InterruptedException e) { fail(); }
        }
        
        for (AssertionFailedError e : errors) {
            throw e;
        }
    }
    
    private AtomicBoolean writerRan = new AtomicBoolean(false);
    public void testWritersSupercedeReaders() {
        
        final int NUM_THREADS = 5;
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i ++) {
            final int index = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        lockManager.lock(LockType.READ, "abc", index);
                        owners.incrementAndGet();
                        assertTrue(lockManager.ownsLock(LockType.READ, "abc", index));
                        assertFalse(lockManager.ownsLock(LockType.WRITE, "abc", index));
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { fail(); }
                        
                        assertEquals(NUM_THREADS, owners.get());
    
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) { fail(); }
                        lockManager.unlock("abc", index);
                        owners.decrementAndGet();
                    } catch (AssertionFailedError e) {
                        errors.add(e);
                    }
                }
            };
            threads[i].start();
        }
        
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) { fail(); }
        
        // WRITER THREAD queues for lock.
        Thread writerThread = new Thread() {
            @Override
            public void run() {
                try {
                    int mySessionId = 100;
                    // Assert the readers own the lock.
                    assertEquals(NUM_THREADS, owners.get());
                    
                    lockManager.lock(LockType.WRITE, "abc", mySessionId);
                    owners.incrementAndGet();
                    assertTrue(lockManager.ownsLock(LockType.READ, "abc", mySessionId));
                    assertTrue(lockManager.ownsLock(LockType.WRITE, "abc", mySessionId));
                    
                    writerRan.set(true);
                    assertEquals(1, owners.get());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) { fail(); }
                    lockManager.unlock("abc", mySessionId);
                    owners.decrementAndGet();
                } catch (AssertionFailedError e) {
                    errors.add(e);
                }
            }
        };
        writerThread.start();
        
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) { fail(); }

        
        // READER THREAD should queue for lock (though currently in READ mode).
        Thread lateReaderThread = new Thread() {
            @Override
            public void run() {
                try {
                    int mySessionId = 101;
                    // Assert the readers still own the lock.
                    assertEquals(NUM_THREADS, owners.get());
                    
                    lockManager.lock(LockType.READ, "abc", mySessionId);
                    owners.incrementAndGet();
                    assertTrue(lockManager.ownsLock(LockType.READ, "abc", mySessionId));
                    assertFalse(lockManager.ownsLock(LockType.WRITE, "abc", mySessionId));
                    
                    // Assert we're the only one who actually gets the lock.
                    assertEquals(1, owners.get());
                    assertTrue(writerRan.get());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) { fail(); }
                    lockManager.unlock("abc", mySessionId);
                    owners.decrementAndGet();
                } catch (AssertionFailedError e) {
                    errors.add(e);
                }
            }
        };
        lateReaderThread.start();
        
        try {
            for (Thread t : threads) { 
                t.join();
            }
            writerThread.join();
            lateReaderThread.join();
        } catch (InterruptedException e) { fail(); }
        
        for (AssertionFailedError e : errors) {
            throw e;
        }
    }

    
    private AtomicBoolean readersRan = new AtomicBoolean(false);
    public void testReadersSupercedeWriters() {
        
        final int NUM_THREADS = 5;
        Thread writerThread1 = new Thread() {
            @Override
            public void run() {
                try {
                    int mySessionId = 100;
                    assertEquals(0, owners.get());
                    
                    lockManager.lock(LockType.WRITE, "abc", mySessionId);
                    owners.incrementAndGet();
                    assertTrue(lockManager.ownsLock(LockType.READ, "abc", mySessionId));
                    assertTrue(lockManager.ownsLock(LockType.WRITE, "abc", mySessionId));
                    
                    assertEquals(1, owners.get());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) { fail(); }
                    owners.decrementAndGet();
                    lockManager.unlock("abc", mySessionId);
                } catch (AssertionFailedError e) {
                    errors.add(e);
                }
            }
        };
        writerThread1.start();
        
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) { fail(); }
        
        // READER threads queue for lock.
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i ++) {
            final int index = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    try {
                        assertEquals(1, owners.get());
                        lockManager.lock(LockType.READ, "abc", index);
                        owners.incrementAndGet();
                        assertTrue(lockManager.ownsLock(LockType.READ, "abc", index));
                        assertFalse(lockManager.ownsLock(LockType.WRITE, "abc", index));
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) { fail(); }
                        
                        assertEquals(NUM_THREADS, owners.get());
                        readersRan.set(true);
    
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) { fail(); }
                        owners.decrementAndGet();
                        lockManager.unlock("abc", index);
                    } catch (AssertionFailedError e) {
                        errors.add(e);
                    }
                }
            };
            threads[i].start();
        }
        
        try {
            Thread.sleep(250);
        } catch (InterruptedException e) { fail(); }

        
        // WRITER thread should queue behind all readers.
        Thread writerThread2 = new Thread() {
            @Override
            public void run() {
                try {
                    int mySessionId = 101;
                    
                    lockManager.lock(LockType.WRITE, "abc", mySessionId);
                    owners.incrementAndGet();
                    assertTrue(lockManager.ownsLock(LockType.READ, "abc", mySessionId));
                    assertTrue(lockManager.ownsLock(LockType.WRITE, "abc", mySessionId));
                    
                    assertEquals(1, owners.get());
                    assertTrue(readersRan.get());

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) { fail(); }
                    owners.decrementAndGet();
                    lockManager.unlock("abc", mySessionId);
                } catch (AssertionFailedError e) {
                    errors.add(e);
                }
            }
        };
        writerThread2.start();
        
        try {
            for (Thread t : threads) { 
                t.join();
            }
            writerThread1.join();
            writerThread2.join();
        } catch (InterruptedException e) { fail(); }
        
        for (AssertionFailedError e : errors) {
            throw e;
        }
    }
}

