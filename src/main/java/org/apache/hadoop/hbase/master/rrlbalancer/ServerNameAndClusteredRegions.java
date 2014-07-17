package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * Used by
 * {@link RelatedRegionsLoadBalancer#balanceClusterByMovingRelatedRegions(Map)}
 * to figure out related regions which are placed on different region servers.
 * 
 */
class ServerNameAndClusteredRegions {

	private final ServerName serverName;
	private final RegionClusterKey regionClusterKey;
	private final List<HRegionInfo> clusteredRegions;
	private final int clusterSize;

	public ServerNameAndClusteredRegions(ServerName serverName,
			RegionClusterKey regionStartKeyEndKey,
			List<HRegionInfo> clusteredRegions) {
		this.serverName = serverName;
		this.regionClusterKey = regionStartKeyEndKey;
		this.clusteredRegions = clusteredRegions;
		clusterSize = clusteredRegions.size();
	}

	public ServerName getServerName() {
		return serverName;
	}

	public RegionClusterKey getRegionClusterKey() {
		return regionClusterKey;
	}

	public List<HRegionInfo> getClusteredRegions() {
		return clusteredRegions;
	}

	/**
	 * {@link #compareTo(ServerNameAndClusteredRegions)} is using only
	 * regionClusterKey and clusterSize for comparing the another instance of
	 * this object.
	 * 
	 */
	public static class ServerNameAndClusteredRegionsComparator implements
			Comparator<ServerNameAndClusteredRegions> {

		@Override
		public int compare(ServerNameAndClusteredRegions first,
				ServerNameAndClusteredRegions second) {
			// TODO Auto-generated method stub
			int compRes = first.regionClusterKey
					.compareTo(second.regionClusterKey);
			if (compRes != 0)
				return compRes;
			return first.clusterSize - second.clusterSize;

		}
	}

}
