package edu.berkeley.thebes.common.data;

import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.thrift.ThriftDataItem;

public class DataItem implements Comparable<DataItem> {
	private final ThriftDataItem thriftDataItem;

	public DataItem(byte[] data, Version version, List<String> transactionKeys) {
        thriftDataItem = new ThriftDataItem();
        thriftDataItem.setData(data);
        thriftDataItem.setVersion(version.getThriftVersion());
        thriftDataItem.setTransactionKeys(transactionKeys);
        assert(thriftDataItem.getVersion() != null);
	}

    public DataItem(ThriftDataItem item) {
        assert(item.getVersion() != null);
        thriftDataItem = item;
    }
	
	public DataItem(ByteBuffer data, Version version) {
        thriftDataItem = new ThriftDataItem();
        thriftDataItem.setData(data);
        thriftDataItem.setVersion(version.getThriftVersion());
        assert(thriftDataItem.getVersion() != null);
    }
	
	public ThriftDataItem toThrift() {
		return thriftDataItem;
	}

	public ByteBuffer getData() {
        if(thriftDataItem.getData() == null)
            return null;

		return ByteBuffer.wrap(thriftDataItem.getData());
	}

	public Version getVersion() {
		return Version.fromThrift(thriftDataItem.getVersion());
	}

    public Version setVersion(Version newVersion) {
        thriftDataItem.setVersion(newVersion.getThriftVersion());
        return newVersion;
    }

	public List<String> getTransactionKeys() {
		return thriftDataItem.getTransactionKeys();
	}

	public void setTransactionKeys(List<String> transactionKeys) {
		thriftDataItem.setTransactionKeys(transactionKeys);
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(thriftDataItem.getData(),
                                // is this what we want here?
                                this.getVersion(),
                                thriftDataItem.getTransactionKeys());
	}

	@Override
	public boolean equals(Object other) {
		if (! (other instanceof DataItem)) {
			return false;
		}
		
		DataItem di = (DataItem) other;
		return Objects.equal(getData(), di.getData()) &&
				Objects.equal(getVersion(), di.getVersion()) &&
				Objects.equal(getTransactionKeys(), di.getTransactionKeys());
	}

    @Override
    public int compareTo(DataItem o) {
        return ComparisonChain.start()
                .compare(this.getVersion(), o.getVersion())
                .compare(this.getData(), o.getData())
                .result();
    }
}
