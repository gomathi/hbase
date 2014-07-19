package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

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
	private final String startkey;
	private final String endKey;
	private final String tableName;

	public ServerNameAndClusteredRegions(ServerName serverName,
			RegionClusterKey regionClusterKey,
			List<HRegionInfo> clusteredRegions) {
		this.serverName = serverName;
		this.regionClusterKey = regionClusterKey;
		this.clusteredRegions = clusteredRegions;
		clusterSize = clusteredRegions.size();
		this.startkey = Bytes.toString(regionClusterKey.startKey);
		this.endKey = Bytes.toString(regionClusterKey.endKey);
		tableName = regionClusterKey.clusterName;
	}

	/**
	 * This is used to identify servername of the clustered regions.
	 * 
	 * @return
	 */
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
