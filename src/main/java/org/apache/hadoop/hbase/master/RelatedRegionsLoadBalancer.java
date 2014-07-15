package org.apache.hadoop.hbase.master;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * 
 * A load balancer which makes sure related regions are placed on the same
 * Region server. Without this, related regions might be placed on different
 * Region servers, and will cause increased latency during the processing.
 * 
 * For this class to do its functionality, it needs to be provided with related
 * tables information. Look at {@link #RelatedRegionsLoadBalancer(List)}.
 */

public class RelatedRegionsLoadBalancer extends DefaultLoadBalancer {
	private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
	private static final Random RANDOM = new Random(System.currentTimeMillis());
	private static final TableClusterMapping TABLE_CLUSTER_MAPPING = new TableClusterMappingImpl();

	private static interface ClusterDataKeyGenerator<V, K> {
		K generateKey(V v);
	}

	/**
	 * Used by
	 * {@link RelatedRegionsLoadBalancer#balanceClusterByMovingRelatedRegions(Map)}
	 * to figure out related regions servers which are placed onto different
	 * region servers.
	 * 
	 */
	private static class ServerNameAndClusteredRegions implements
			Comparable<ServerNameAndClusteredRegions> {

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

		@Override
		public int compareTo(ServerNameAndClusteredRegions o) {
			// TODO Auto-generated method stub
			int compRes = regionClusterKey.compareTo(o.regionClusterKey);
			if (compRes != 0)
				return compRes;
			return clusterSize - o.clusterSize;
		}

	}

	/**
	 * A holder class to count total no of reassigned regions during
	 * {@link RelatedRegionsLoadBalancer#retainAssignment(Map, List)}
	 * 
	 */
	private static interface ReassignedRegionsCounter {
		void incrCounterBy(int value);

		int getCurrCount();
	}

	/**
	 * Given a table name, gives the corresponding cluster name. The user has to
	 * initialize with the related tables information.
	 * 
	 */
	private static interface TableClusterMapping {
		void addClusters(List<Set<String>> clusters);

		void addCluster(Set<String> cluster);

		boolean isPartOfAnyCluster(String tableName);

		String getClusterName(String tableName);
	}

	private static class TableClusterMappingImpl implements TableClusterMapping {

		private final static String CLUSTER_PREFIX = "cluster-";
		private final static String RANDOM_PREFIX = "random-";

		private Map<Integer, String> clusterIdAndName = new HashMap<Integer, String>();
		private List<Set<String>> clusters = new ArrayList<Set<String>>();

		public void addClusters(List<Set<String>> clusters) {
			for (Set<String> cluster : clusters)
				addCluster(cluster);
		}

		public void addCluster(Set<String> cluster) {
			clusters.add(cluster);
			String currClusterName = CLUSTER_PREFIX + clusters.size();
			clusterIdAndName.put(clusters.size(), currClusterName);
		}

		private int getClusterIndexOf(String tableName) {
			for (int i = 0; i < clusters.size(); i++) {
				if (clusters.get(i).contains(tableName)) {
					return i;
				}
			}
			return -1;
		}

		public boolean isPartOfAnyCluster(String tableName) {
			return (getClusterIndexOf(tableName) != -1) ? true : false;
		}

		public String getClusterName(String tableName) {
			int clusterIndex = getClusterIndexOf(tableName);
			if (clusterIndex != -1)
				return clusterIdAndName.get(clusterIndex);
			return RANDOM_PREFIX + RANDOM.nextInt();
		}
	}

	/**
	 * If two regions share same startkey, endkey, they are called as region
	 * clusters.
	 * 
	 */
	private static class RegionClusterKey implements
			Comparable<RegionClusterKey> {
		private final byte[] startKey, endKey;
		private final String clusterName;

		public RegionClusterKey(String clusterName, byte[] startKey,
				byte[] endKey) {
			this.startKey = startKey;
			this.endKey = endKey;
			this.clusterName = clusterName;
		}

		@Override
		public int hashCode() {
			int prime = 31;
			int result = Arrays.hashCode(startKey);
			result = result * prime + Arrays.hashCode(endKey);
			result = result * prime + clusterName.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object anoObj) {
			if (anoObj == null || !(anoObj instanceof RegionClusterKey))
				return false;
			if (this == anoObj)
				return true;
			RegionClusterKey regionStartEndKeyObj = (RegionClusterKey) anoObj;
			if (Arrays.equals(startKey, regionStartEndKeyObj.startKey)
					&& Arrays.equals(endKey, regionStartEndKeyObj.endKey)
					&& clusterName.equals(regionStartEndKeyObj.clusterName))
				return true;
			return false;
		}

		@Override
		public int compareTo(RegionClusterKey o) {
			// TODO Auto-generated method stub
			int compRes = clusterName.compareTo(o.clusterName);
			if (compRes != 0)
				return compRes;
			compRes = Bytes.compareTo(startKey, o.startKey);
			if (compRes != 0)
				return compRes;
			compRes = Bytes.compareTo(endKey, o.endKey);
			return compRes;
		}
	}

	private static final ClusterDataKeyGenerator<HRegionInfo, RegionClusterKey> REGION_KEY_GEN = new ClusterDataKeyGenerator<HRegionInfo, RegionClusterKey>() {

		@Override
		public RegionClusterKey generateKey(HRegionInfo hregionInfo) {
			// TODO Auto-generated method stub
			return new RegionClusterKey(
					TABLE_CLUSTER_MAPPING.getClusterName(hregionInfo
							.getTableNameAsString()),
					hregionInfo.getStartKey(), hregionInfo.getEndKey());
		}

	};

	private static final ClusterDataKeyGenerator<ServerName, String> SERVER_KEY_GEN = new ClusterDataKeyGenerator<ServerName, String>() {

		@Override
		public String generateKey(ServerName serverName) {
			// TODO Auto-generated method stub
			return serverName.getHostname();
		}

	};

	public RelatedRegionsLoadBalancer(List<Set<String>> clusteredTableNamesList) {
		TABLE_CLUSTER_MAPPING.addClusters(clusteredTableNamesList);
	}

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

	/**
	 * First step of {@link #balanceCluster(Map)}. If related regions are shared
	 * between two or more region servers, those related regions will be moved
	 * to one region server.
	 * 
	 * @param clusterState
	 * @return
	 */
	private List<RegionPlan> balanceClusterByMovingRelatedRegions(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		List<RegionPlan> result = new ArrayList<RegionPlan>();
		Set<ServerNameAndClusteredRegions> sortedServerNamesAndClusteredRegions = new TreeSet<RelatedRegionsLoadBalancer.ServerNameAndClusteredRegions>();
		for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
				.entrySet()) {
			ServerName serverName = entry.getKey();
			Map<RegionClusterKey, List<HRegionInfo>> clusteredRegions = clusterRegionsAndGetMap(entry
					.getValue());
			for (Map.Entry<RegionClusterKey, List<HRegionInfo>> innerEntry : clusteredRegions
					.entrySet()) {
				sortedServerNamesAndClusteredRegions
						.add(new ServerNameAndClusteredRegions(serverName,
								innerEntry.getKey(), innerEntry.getValue()));
			}
		}

		return result;
	}

	@Override
	public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(
			List<HRegionInfo> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();

		int numServers = servers.size();
		int numRegions = regions.size();
		int maxSize = numRegions / numServers;
		List<List<HRegionInfo>> clusteredRegions = clusterRegions(regions);
		Iterator<List<HRegionInfo>> itr = clusteredRegions.iterator();
		for (ServerName server : servers) {
			while (itr.hasNext()) {
				List<HRegionInfo> input = itr.next();
				if (!result.containsKey(server))
					result.put(server, new ArrayList<HRegionInfo>());
				result.get(server).addAll(input);
				if (result.get(server).size() >= maxSize)
					break;
			}
		}

		return result;
	}

	/**
	 * This is called during the cluster initialization.
	 * 
	 * 1) Tries to retain the existing region servers and regions mappings 2)
	 * Makes sure related regions are placed on the same region servers.
	 * 
	 * The algorithms is as following
	 * 
	 * <ol>
	 * 
	 * <li>Cluster the servers based on hostname, and cluster the regions based
	 * on (key range, and related table group id).
	 * 
	 * <li>For each clustered region group, figure out whether any existing
	 * hosting region server of the region is dead, or figure out if regions are
	 * placed on different hosts. If yes, try to allocate a clustered server
	 * group for the unplaced regions, and also move the existing regions to a
	 * clustered server group where already the majority of the regions are
	 * living. This step is handled by internal method
	 * {@link #retainAssignmentCluster(Map, Map, List, ReassignedRegionsCounter)}
	 * 
	 * </ol>
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
		ReassignedRegionsCounter reassignedCntr = new ReassignedRegionsCounter() {

			int reassignedCnt = 0;

			@Override
			public void incrCounterBy(int value) {
				// TODO Auto-generated method stub
				reassignedCnt += value;
			}

			@Override
			public int getCurrCount() {
				// TODO Auto-generated method stub
				return reassignedCnt;
			}
		};

		List<ServerName> allUnavailServers = minus(regions.values(), servers);
		Map<String, List<ServerName>> allAvailClusteredServers = clusterServers(servers);
		Collection<List<HRegionInfo>> allClusteredRegionGroups = clusterRegions(regions
				.keySet());

		for (List<HRegionInfo> clusteredRegionGroup : allClusteredRegionGroups) {
			Map<HRegionInfo, ServerName> clusterdRegionAndServerNameMap = getMapEntriesForKeys(
					regions, clusteredRegionGroup);

			Map<ServerName, List<HRegionInfo>> partialResult = retainAssignmentCluster(
					clusterdRegionAndServerNameMap, allAvailClusteredServers,
					allUnavailServers, reassignedCntr);

			for (Map.Entry<ServerName, List<HRegionInfo>> partialEntry : partialResult
					.entrySet()) {
				if (!result.containsKey(partialEntry.getKey()))
					result.put(partialEntry.getKey(),
							new ArrayList<HRegionInfo>());
				result.get(partialEntry.getKey()).addAll(
						partialEntry.getValue());
			}

		}
		LOG.info("No of unavailable servers which were previously assigned to regions : "
				+ allUnavailServers.size()
				+ "and unavailable hosts are"
				+ Joiner.on("\n").join(allUnavailServers));

		LOG.info("Total no of reassigned regions : "
				+ reassignedCntr.getCurrCount());
		return result;
	}

	private Map<ServerName, List<HRegionInfo>> retainAssignmentCluster(
			Map<HRegionInfo, ServerName> clusteredRegionAndServerNameMap,
			Map<String, List<ServerName>> allAvailClusteredServers,
			List<ServerName> allUnavailServers,
			ReassignedRegionsCounter reassignedCntr) {
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
		ListMultimap<ServerName, HRegionInfo> localServerNameAndClusteredRegions = reverseMap(clusteredRegionAndServerNameMap);

		List<ServerName> localServers = new ArrayList<ServerName>(
				clusteredRegionAndServerNameMap.values());
		List<ServerName> localUnavailServers = intersect(allUnavailServers,
				localServers);
		List<ServerName> localAvailServers = minus(localServers,
				localUnavailServers);
		List<HRegionInfo> localUnplacedRegions = getMapEntriesForKeys(
				localServerNameAndClusteredRegions, localUnavailServers);

		for (ServerName unavailServer : localUnavailServers)
			localServerNameAndClusteredRegions.removeAll(unavailServer);

		List<List<ServerName>> bestPlacementRegionClusters;
		if (localAvailServers.size() == 0)
			bestPlacementRegionClusters = new ArrayList<List<ServerName>>(
					allAvailClusteredServers.values());
		else {
			String hostWithMajorityRegions = findHostWithMajorityRegions(localServerNameAndClusteredRegions);
			bestPlacementRegionClusters = new ArrayList<List<ServerName>>();
			bestPlacementRegionClusters.add(allAvailClusteredServers
					.get(hostWithMajorityRegions));

			for (ServerName server : localServerNameAndClusteredRegions
					.keySet()) {
				if (!server.getHostname().equals(hostWithMajorityRegions))
					localUnplacedRegions
							.addAll(localServerNameAndClusteredRegions
									.get(server));
				else
					result.put(server,
							localServerNameAndClusteredRegions.get(server));
			}
		}

		Map<HRegionInfo, ServerName> assignment = immediateAssignmentCluster(
				localUnplacedRegions, bestPlacementRegionClusters);
		for (Map.Entry<HRegionInfo, ServerName> entry : assignment.entrySet()) {
			if (!result.containsKey(entry.getValue()))
				result.put(entry.getValue(), new ArrayList<HRegionInfo>());
			result.get(entry.getValue()).add(entry.getKey());
		}

		reassignedCntr.incrCounterBy(localUnplacedRegions.size());
		return result;
	}

	/**
	 * Returns the server which hosts many regions. This function is mainly used
	 * to find server name to host related regions. It makes sense to place a
	 * related region on a host where already sister regions are living.
	 * 
	 * @param serverNameAndRegionsMap
	 * @return
	 */
	private String findHostWithMajorityRegions(
			ListMultimap<ServerName, HRegionInfo> serverNameAndRegionsMap) {
		String result = null;
		int maxRegionsCount = -1;
		int localCount = -1;
		Map<String, Integer> serverNameAndRegionCount = new HashMap<String, Integer>();
		for (ServerName serverName : serverNameAndRegionsMap.keySet()) {
			if (!serverNameAndRegionCount.containsKey(serverName.getHostname()))
				serverNameAndRegionCount.put(serverName.getHostname(), 0);

			localCount = serverNameAndRegionCount.get(serverName.getHostname())
					+ serverNameAndRegionsMap.get(serverName).size();
			serverNameAndRegionCount.put(serverName.getHostname(), localCount);

			if (localCount > maxRegionsCount) {
				maxRegionsCount = localCount;
				result = serverName.getHostname();
			}
		}

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
		List<List<HRegionInfo>> clusteredRegionGroups = clusterRegions(regions);
		List<List<ServerName>> clusterdServerGroups = new ArrayList<List<ServerName>>(
				clusterServers(servers).values());
		Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();

		for (List<HRegionInfo> clusterRegionGroup : clusteredRegionGroups) {
			assignments.putAll(immediateAssignmentCluster(clusterRegionGroup,
					clusterdServerGroups));
		}
		return assignments;
	}

	private Map<HRegionInfo, ServerName> immediateAssignmentCluster(
			List<HRegionInfo> clusteredRegionGroup,
			List<List<ServerName>> clusterdServerGroups) {
		// TODO Auto-generated method stub
		Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();

		List<ServerName> clusteredServerGroup = findBestCluster(clusterdServerGroups);
		int i = 0;
		int size = clusteredServerGroup.size();
		for (HRegionInfo hRegionInfo : clusteredRegionGroup) {
			assignments.put(hRegionInfo, clusteredServerGroup.get(i));
			i = (i + 1) % size;
		}

		return assignments;
	}

	/**
	 * Current implementation gives a random cluster group from all the groups.
	 * 
	 * TODO: Using history metrics of clustered server groups, we can find the
	 * best cluster server group.
	 * 
	 * @param allClusteredServerGroups
	 * @return
	 */
	private List<ServerName> findBestCluster(
			List<List<ServerName>> allClusteredServerGroups) {
		return allClusteredServerGroups.get(RANDOM
				.nextInt(allClusteredServerGroups.size()));
	}

	@Override
	public ServerName randomAssignment(List<ServerName> servers) {
		// TODO Auto-generated method stub
		return super.randomAssignment(servers);
	}

	/**
	 * Groups the servers based on the host. If multiple region servers are
	 * running on the same host, then this will group them into a cluster.
	 * 
	 * @param servers
	 * @return
	 */

	private static Map<String, List<ServerName>> clusterServers(
			Collection<ServerName> servers) {
		return cluster(servers, SERVER_KEY_GEN);
	}

	/**
	 * Groups the regions based on the region start key and region end key.
	 * 
	 * @param regions
	 * @return
	 */
	private static List<List<HRegionInfo>> clusterRegions(
			Collection<HRegionInfo> regions) {
		return new ArrayList<List<HRegionInfo>>(
				cluster(regions, REGION_KEY_GEN).values());
	}

	private static Map<RegionClusterKey, List<HRegionInfo>> clusterRegionsAndGetMap(
			Collection<HRegionInfo> regions) {
		return cluster(regions, REGION_KEY_GEN);
	}

	// Common functions

	private static <K, V> Map<K, List<V>> cluster(Collection<V> input,
			ClusterDataKeyGenerator<V, K> keyGenerator) {
		Map<K, List<V>> result = new HashMap<K, List<V>>();
		for (V v : input) {
			K k = keyGenerator.generateKey(v);
			if (!result.containsKey(k))
				result.put(k, new ArrayList<V>());
			result.get(k).add(v);
		}

		return result;
	}

	private static <K, V> List<V> getMapEntriesForKeys(ListMultimap<K, V> map,
			Collection<K> keys) {
		List<V> result = new ArrayList<V>();
		for (K key : keys)
			if (map.containsKey(key))
				result.addAll(map.get(key));
		return result;
	}

	private static <K, V> Map<K, V> getMapEntriesForKeys(Map<K, V> map,
			Collection<K> keys) {
		Map<K, V> result = new HashMap<K, V>();
		for (K key : keys)
			if (map.containsKey(key))
				result.put(key, map.get(key));
		return result;
	}

	/**
	 * Returns the common elements of a and b. Note: Result is not a mutli bag
	 * operation, the input collections are converted into set equivalents.
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	private static <T> List<T> intersect(Collection<T> a, Collection<T> b) {
		Set<T> hashedEntriesA = new HashSet<T>();
		hashedEntriesA.addAll(a);

		Set<T> result = new HashSet<T>();
		for (T in : b)
			if (hashedEntriesA.contains(in))
				result.add(in);
		return new ArrayList<T>(result);
	}

	/**
	 * Returns the result of (a - b). Note: Result is not a mutli bag operation,
	 * the input collections are converted into set equivalents.
	 * 
	 * @param a
	 * @param b
	 * @return
	 */
	private static <T> List<T> minus(Collection<T> a, Collection<T> b) {
		Set<T> hashedEntriesB = new HashSet<T>();
		hashedEntriesB.addAll(b);

		Set<T> result = new HashSet<T>();
		for (T serverName : a)
			if (!hashedEntriesB.contains(serverName))
				result.add(serverName);
		return new ArrayList<T>(result);
	}

	private static <K, V> ListMultimap<V, K> reverseMap(Map<K, V> input) {
		ArrayListMultimap<V, K> multiMap = ArrayListMultimap.create();
		for (Map.Entry<K, V> entry : input.entrySet()) {
			multiMap.put(entry.getValue(), entry.getKey());
		}

		return multiMap;
	}
}
