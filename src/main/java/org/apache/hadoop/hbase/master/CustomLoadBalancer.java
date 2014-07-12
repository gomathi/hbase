package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

/**
 * 
 * A force load balancer which makes sure related regions are placed on the same
 * Region server. Without this, related regions might be placed on different
 * Region servers, and will cause increased latency during the processing.
 * 
 * 
 * TODO: This implementation assumes all tables are related to each other. Tries
 * to place regions of the tables on the same region server. If an unrelated
 * table will be managed by the Force server, then we might need to revisit this
 * class to change the placement logic for those unrelated regions.
 */

public class CustomLoadBalancer extends DefaultLoadBalancer {
	private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
	private static final Random RANDOM = new Random(System.currentTimeMillis());
	private static final ClusterDataKeyGenerator<HRegionInfo, RegionStartEndKeyId> REGION_KEY_GEN = new ClusterDataKeyGenerator<HRegionInfo, RegionStartEndKeyId>() {

		@Override
		public RegionStartEndKeyId generateKey(HRegionInfo hregionInfo) {
			// TODO Auto-generated method stub
			return new RegionStartEndKeyId(hregionInfo.getStartKey(),
					hregionInfo.getEndKey());
		}

	};

	private static final ClusterDataKeyGenerator<ServerName, String> SERVER_KEY_GEN = new ClusterDataKeyGenerator<ServerName, String>() {

		@Override
		public String generateKey(ServerName serverName) {
			// TODO Auto-generated method stub
			return serverName.getHostname();
		}

	};

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return super.getConf();
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		super.setConf(conf);
	}

	@Override
	public void setClusterStatus(ClusterStatus st) {
		// TODO Auto-generated method stub
		super.setClusterStatus(st);
	}

	@Override
	public void setMasterServices(MasterServices masterServices) {
		// TODO Auto-generated method stub
		super.setMasterServices(masterServices);
	}

	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * This is called during the cluster initialization. It is critical to
	 * provide the correct placement logic.
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
		for (ServerName server : servers)
			result.put(server, new ArrayList<HRegionInfo>());

		Set<ServerName> unavailableServers = minus(servers, regions.values());
		Collection<List<HRegionInfo>> clusteredRegions = clusterRegions(regions
				.keySet());

		for (List<HRegionInfo> cluster : clusteredRegions) {
			Map<HRegionInfo, ServerName> regionInfoAndServerForCluster = getEntriesFrom(
					regions, cluster);
			List<ServerName> clusterServers = new ArrayList<ServerName>(
					removeDuplicates(regionInfoAndServerForCluster.values()));
			Set<ServerName> unavailableServersForCluster = intersect(
					unavailableServers, clusterServers);
			Collections.sort(clusterServers, new UnavailableServerComparator(
					unavailableServersForCluster));

			int indFirstAvailServer = 0;
			int indLastAvailServer = clusterServers.size()
					- unavailableServersForCluster.size() - 1;

			if (clusterServers
					.get(indFirstAvailServer)
					.getHostname()
					.compareTo(
							clusterServers.get(indLastAvailServer)
									.getHostname()) == 0) {

			} else {

			}
		}
		return result;
	}

	private static <A> Collection<A> removeDuplicates(Collection<A> input) {
		Set<A> set = new HashSet<A>();
		set.addAll(input);
		input.clear();
		input.addAll(set);
		return input;
	}

	/**
	 * Used for sorting list of {@link ServerName}, with additional parameter
	 * unavailable servers. Given two servers fServer and sServer, if just one
	 * of them in the unavailable servers list, and it is returned to be greater
	 * element among the two.
	 * 
	 * Usage: In a given collection of servers and sorting the collection using
	 * this comparator, will make the unavailable servers always bubbled to the
	 * end of the list during the sorting.
	 * 
	 */
	private static class UnavailableServerComparator implements
			Comparator<ServerName> {

		private final Set<ServerName> missingServers;

		public UnavailableServerComparator(Set<ServerName> missingServers) {
			this.missingServers = missingServers;
		}

		@Override
		public int compare(ServerName fServer, ServerName sServer) {
			// TODO Auto-generated method stub
			int fExists = missingServers.contains(fServer) ? 1 : 0;
			int sExists = missingServers.contains(sServer) ? 1 : 0;
			if (fExists == sExists)
				return fServer.compareTo(sServer);
			return fExists - sExists;
		}

	}

	private static <K, V> Map<K, V> getEntriesFrom(Map<K, V> map,
			Collection<K> keys) {
		Map<K, V> result = new HashMap<K, V>();
		for (K key : keys)
			if (map.containsKey(key))
				result.put(key, map.get(key));
		return result;
	}

	/**
	 * Returns the common elements of a and b
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	private static Set<ServerName> intersect(Collection<ServerName> a,
			Collection<ServerName> b) {
		Set<ServerName> hashedEntriesA = new HashSet<ServerName>();
		hashedEntriesA.addAll(a);

		Set<ServerName> result = new HashSet<ServerName>();
		for (ServerName serverName : b)
			if (hashedEntriesA.contains(serverName))
				result.add(serverName);
		return result;
	}

	/**
	 * Returns the result of (a - b)
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	private static Set<ServerName> minus(Collection<ServerName> a,
			Collection<ServerName> b) {
		Set<ServerName> hashedEntriesB = new HashSet<ServerName>();
		hashedEntriesB.addAll(b);

		Set<ServerName> result = new HashSet<ServerName>();
		for (ServerName serverName : a)
			if (!hashedEntriesB.contains(serverName))
				result.add(serverName);
		return result;
	}

	/**
	 * Immediate assignment of regions to servers. Does not consider the best
	 * way to assign regions. Makes sure related regions are assigned to the
	 * same region server.
	 */
	@Override
	public Map<HRegionInfo, ServerName> immediateAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Collection<List<HRegionInfo>> clusteredRegionGroups = clusterRegions(regions);
		List<List<ServerName>> clusterdServerGroups = clusterServers(servers);
		Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();

		for (List<HRegionInfo> clusterRegionGroup : clusteredRegionGroups) {
			List<ServerName> clusteredServerGroup = clusterdServerGroups
					.get(RANDOM.nextInt(clusterdServerGroups.size()));
			int i = 0;
			int size = clusteredServerGroup.size();
			for (HRegionInfo hRegionInfo : clusterRegionGroup) {
				assignments.put(hRegionInfo, clusteredServerGroup.get(i));
				i = (i + 1) % size;
			}
		}
		return assignments;
	}

	@Override
	public ServerName randomAssignment(List<ServerName> servers) {
		// TODO Auto-generated method stub
		return super.randomAssignment(servers);
	}

	private static interface ClusterDataKeyGenerator<V, K> {
		K generateKey(V v);
	}

	private static <K, V> List<List<V>> cluster(Collection<V> input,
			ClusterDataKeyGenerator<V, K> keyGenerator) {
		Map<K, List<V>> result = new HashMap<K, List<V>>();
		for (V v : input) {
			K k = keyGenerator.generateKey(v);
			if (!result.containsKey(k))
				result.put(k, new ArrayList<V>());
			result.get(k).add(v);
		}

		return new ArrayList<List<V>>(result.values());
	}

	/**
	 * Groups the servers based on the hostname. If multiple region servers are
	 * running on the same host, then this will group them into a group.
	 * 
	 * @param servers
	 * @return
	 */

	private static List<List<ServerName>> clusterServers(
			Collection<ServerName> servers) {
		return cluster(servers, SERVER_KEY_GEN);
	}

	/**
	 * Groups the regions based on the region start key and region end key.
	 * 
	 * @param regions
	 * @return
	 */
	private static Collection<List<HRegionInfo>> clusterRegions(
			Collection<HRegionInfo> regions) {
		return cluster(regions, REGION_KEY_GEN);
	}

	private static class RegionStartEndKeyId {
		private final byte[] startKey, endKey;

		public RegionStartEndKeyId(byte[] startKey, byte[] endKey) {
			this.startKey = startKey;
			this.endKey = endKey;
		}

		@Override
		public int hashCode() {
			int prime = 31;
			int result = Arrays.hashCode(startKey);
			result = result * prime + Arrays.hashCode(endKey);
			return result;
		}

		@Override
		public boolean equals(Object anoObj) {
			if (anoObj == null || !(anoObj instanceof RegionStartEndKeyId))
				return false;
			if (this == anoObj)
				return true;
			RegionStartEndKeyId regionStartEndKeyObj = (RegionStartEndKeyId) anoObj;
			if (Arrays.equals(startKey, regionStartEndKeyObj.startKey)
					&& Arrays.equals(endKey, regionStartEndKeyObj.endKey))
				return true;
			return false;
		}
	}

}
