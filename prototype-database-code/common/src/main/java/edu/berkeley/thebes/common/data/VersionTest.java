package edu.berkeley.thebes.common.data;

import junit.framework.TestCase;

public class VersionTest extends TestCase {
    private static final int numBitsTimestamp = 29;
    private static final int numBitsLogicalTime = 27;
    private static final int numBitsClientID = 8;
    
    /** Constructs a new Version! */
    public static Version v(long clientID, long logicalTime, long timestamp) { 
        return new Version((short) clientID, logicalTime, timestamp);
    }

    public void testInAndOut() {
        Version v1, v2;
        
        v1 = v(1, 23, 456);
        assertTrue(v1.getClientID() == 1);
        assertTrue(v1.getLogicalTime() == 23);
        assertTrue(v1.getTimestamp() == 456);
        v2 = Version.fromThrift(Version.toThrift(v1));
        assertSame(v1, v2);
        
        v1 = v(pow2Less1(numBitsClientID), pow2Less1(numBitsLogicalTime),
                pow2Less1(numBitsTimestamp));
        assertTrue(v1.getClientID() == pow2Less1(numBitsClientID));
        assertTrue(v1.getLogicalTime() == pow2Less1(numBitsLogicalTime));
        assertTrue(v1.getTimestamp() == pow2Less1(numBitsTimestamp));
        v2 = Version.fromThrift(Version.toThrift(v1));
        assertSame(v1, v2);
    }
    
    public void testOutOfBounds() {
        Version v1, v2;
        v1 = v(pow2Less1(numBitsClientID)+2, pow2Less1(numBitsLogicalTime)+2,
                pow2Less1(numBitsTimestamp)+2);
        assertTrue(v1.getClientID() == 1);
        assertTrue(v1.getLogicalTime() == 1);
        assertTrue(v1.getTimestamp() == 1);
        
        v2 = Version.fromThrift(Version.toThrift(v1));
        assertSame(v1, v2);
    }
    
    public void testCompare() {
        Version v1 = v(1, 23, 456);
        assertCompare(v1, v(1, 23, 456), 0);
        assertCompare(v1, v(2, 23, 456), -1);
        assertCompare(v1, v(1, 24, 456), -1);
        assertCompare(v1, v(1, 23, 457), -1);
        
        // Test wraparound of timestamps!
        assertCompare(v1, v(1, 23, pow2Less1(numBitsTimestamp)/2), -1);
        assertCompare(v1, v(1, 23, pow2Less1(numBitsTimestamp)/2+457), 1);
        assertCompare(v1, v(1, 23, pow2Less1(numBitsTimestamp)), 1);
        
        // Nothing else should wraparound
        assertCompare(v1, v(1, pow2Less1(numBitsLogicalTime), 456), -1);
        assertCompare(v1, v(pow2Less1(numBitsClientID), 23, 456), -1);
        
        // Ordering: Timestamp, Client, Logical time
        assertCompare(v1, v(1000, 1000, 455), 1);
        assertCompare(v1, v(0, 1000, 456), 1);
        assertCompare(v1, v(1000, 0, 456), -1);
    }
    
    public static void assertCompare(Version v1, Version v2, int expect) {
        assertTrue(v1.compareTo(v2) == expect);
        assertTrue(v2.compareTo(v1) == -expect);
    }
    
    public static void assertSame(Version v1, Version v2) {
        assertEquals(v1, v2);
        assertTrue(v1.compareTo(v2) == 0);
        assertTrue(v2.compareTo(v1) == 0);
        assertEquals(v1.hashCode(), v2.hashCode());
    }
    
    private static long pow2Less1(int b) {
        return Math.round(Math.pow(2, b)) - 1;
    }
}
