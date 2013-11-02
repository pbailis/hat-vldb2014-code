package edu.berkeley.thebes.common.data;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import edu.berkeley.thebes.common.thrift.ThriftVersion;

public class Version implements Comparable<Version> {
    public static final Version NULL_VERSION = new Version((short) -1, -1, -1);

    private ThriftVersion thriftVersion;

    private static final int numBitsTimestamp = 29;   // ~3 days
    private static final int numBitsLogicalTime = 27; // ~128 million operations per client (2-hour run)
    private static final int numBitsClientID = 8;     // 256 clients
    static { assert (numBitsTimestamp + numBitsLogicalTime + numBitsClientID  == 64); }
    
	private final short clientID;
	private final long logicalTime;
	/** Timestamp only carries "numBitsTimestamp" bits, so it's actually circular!
	 * See compareTo() for the rammifications of this. */
	private final long timestamp;
	
	public Version(short clientID, long logicalTime, long timestamp) {
		this.clientID = (short) (pow2Less1(numBitsClientID) & clientID);
		this.logicalTime = pow2Less1(numBitsLogicalTime) & logicalTime;
		this.timestamp = pow2Less1(numBitsTimestamp) & timestamp;
        thriftVersion = Version.toThrift(this);
	}

    public ThriftVersion getThriftVersion() {
        return thriftVersion;
    }

    public static Version fromLong(long version) {
        short clientID = (short) (pow2Less1(numBitsClientID) & version);
        version >>= numBitsClientID;
        long logicalTime = pow2Less1(numBitsLogicalTime) & version;
        version >>= numBitsLogicalTime;
        long timestamp = pow2Less1(numBitsTimestamp) & version;
        return new Version(clientID, logicalTime, timestamp);
    }
	
	public static Version fromThrift(ThriftVersion thriftVersion) {
        if(thriftVersion == null)
            return null;

        return fromLong(thriftVersion.getVersion());
	}
	
	public static ThriftVersion toThrift(Version version) {
        if (version == null || version == NULL_VERSION)
            return null;
        
        long l = 0;
        l |= (pow2Less1(numBitsTimestamp) & version.getTimestamp());
        
        l <<= numBitsLogicalTime;
        l |= pow2Less1(numBitsLogicalTime) & version.getLogicalTime();
        
        l <<= numBitsClientID;
        l |= pow2Less1(numBitsClientID) & version.getClientID();
        return new ThriftVersion(l);
	}
	
	private static long pow2Less1(int b) {
	    return Math.round(Math.pow(2, b)) - 1;
	}

	public short getClientID() {
		return clientID;
	}

    public long getLogicalTime() {
        return logicalTime;
    }

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public int compareTo(Version other) {
	    // If the timestamps are more than half the period (2^numBitsTimestamp) apart,
	    // then we assume we've wrapped around, so the lower is actually higher!
	    if (Math.abs(timestamp - other.getTimestamp()) > pow2Less1(numBitsTimestamp-1)) {
	        return new Long(other.getTimestamp()).compareTo(timestamp);
	    }
	    
	    return ComparisonChain.start()
	            .compare(timestamp, other.getTimestamp())
	            .compare(clientID, other.getClientID())
	            .compare(logicalTime, other.getLogicalTime())
	            .result();
	}

    @Override
    public String toString() {
        return String.format("%d:%d:%d", getClientID(), getLogicalTime(), getTimestamp());
    }
	
	@Override
	public int hashCode() {
		return Objects.hashCode(clientID, logicalTime, timestamp);
	}
	
	@Override
	public boolean equals(Object other) {
		if (! (other instanceof Version)) {
			return false;
		}
		
		Version v = (Version) other;
		return Objects.equal(getClientID(), v.getClientID()) &&
				Objects.equal(getLogicalTime(), v.getLogicalTime()) &&
                Objects.equal(getTimestamp(), v.getTimestamp());
	}
}
