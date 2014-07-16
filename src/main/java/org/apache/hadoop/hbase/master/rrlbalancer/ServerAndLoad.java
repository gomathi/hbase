package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Data structure that holds servername and 'load'.
 */
class ServerAndLoad implements Comparable<ServerAndLoad> {
	private ServerName sn;
	private List<List<HRegionInfo>> clusteredRegions;

	ServerAndLoad(final ServerName sn,
			final List<List<HRegionInfo>> clusteredRegions) {
		this.sn = sn;
		this.clusteredRegions = clusteredRegions;
	}

	ServerName getServerName() {
		return this.sn;
	}

	int getLoad() {
		return this.clusteredRegions.size();
	}

	List<HRegionInfo> removeNextCluster() {
		if (clusteredRegions.size() > 0)
			return clusteredRegions.remove(0);
		throw new NoSuchElementException();
	}

	void addCluster(List<HRegionInfo> cluster) {
		clusteredRegions.add(cluster);
	}

	@Override
	public int compareTo(ServerAndLoad other) {
		int diff = this.getLoad() - other.getLoad();
		return diff != 0 ? diff : this.sn.compareTo(other.getServerName());
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || !(o instanceof ServerAndLoad))
			return false;
		ServerAndLoad obj = (ServerAndLoad) o;
		if (this.sn.equals(obj.sn))
			return true;
		return false;
	}
}