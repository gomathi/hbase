package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Data structure that holds servername and 'load'.
 */
class ServerAndLoad {
	private final ServerName sn;
	private final List<List<HRegionInfo>> clusteredRegions;

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

	public static class ServerAndLoadComparator implements
			Comparator<ServerAndLoad> {

		@Override
		public int compare(ServerAndLoad salFirst, ServerAndLoad salSecond) {
			// TODO Auto-generated method stub
			int diff = salFirst.getLoad() - salSecond.getLoad();
			return diff != 0 ? diff : salFirst.getServerName().compareTo(
					salSecond.getServerName());
		}

	}
}