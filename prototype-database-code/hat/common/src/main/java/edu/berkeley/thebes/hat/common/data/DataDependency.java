package edu.berkeley.thebes.hat.common.data;

import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.hat.common.thrift.ThriftDataDependency;

public class DataDependency {
	private final String key;
	private final Version version;

	public DataDependency(String key, Version version) {
		this.key = key;
		this.version = version;
	}
	
	public static DataDependency fromThrift(ThriftDataDependency dataDependency) {
		return new DataDependency(dataDependency.getKey(),
				Version.fromThrift(dataDependency.getVersion()));
	}
	
	public static List<DataDependency> fromThrift(List<ThriftDataDependency> dataDependencies) {
		if (dataDependencies == null) {
			return null;
		}
		
		List<DataDependency> l = Lists.newArrayListWithCapacity(dataDependencies.size());
		for (ThriftDataDependency dep : dataDependencies) {
			l.add(fromThrift(dep));
		}
		return l;
	}
	
	public static ThriftDataDependency toThrift(DataDependency dataDependency) {
		return new ThriftDataDependency(dataDependency.getKey(),
				Version.toThrift(dataDependency.getVersion()));
	}
	
	public static List<ThriftDataDependency> toThrift(List<DataDependency> dataDependencies) {
		if (dataDependencies == null) {
			return null;
		}
		
		List<ThriftDataDependency> l = Lists.newArrayListWithCapacity(dataDependencies.size());
		for (DataDependency dep : dataDependencies) {
			l.add(toThrift(dep));
		}
		return l;
	}
	
	public String getKey() {
		return key;
	}

	public Version getVersion() {
		return version;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(key, version);
	}

	@Override
	public boolean equals(Object other) {
		if (! (other instanceof DataDependency)) {
			return false;
		}
		
		DataDependency dd = (DataDependency) other;
		return Objects.equal(getKey(), dd.getKey()) &&
				Objects.equal(getVersion(), dd.getVersion());
		}
}
