package org.apache.hadoop.hbase.master;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;

/**
 * 
 * A force load balancer which makes sure related regions are placed on the same
 * Region server. Without this, related regions might be placed on different
 * Region servers, and will cause increased latency during the processing.
 * 
 * 
 * TODO: This implementation assumes all tables are related to each other. Tries
 * to place regions of the tables on the same region server. If an unrelated
 * table will be managed by the Force server, then we may need to revisit this
 * class to change the placement logic for those unrelated regions.
 */

public class CustomLoadBalancer extends DefaultLoadBalancer {
	private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
	private static final Random RANDOM = new Random(System.currentTimeMillis());

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
	 * This is called during the cluster initialization.
	 * 
	 * 1) Tries to retain the existing region servers and regions mappings 2)
	 * Makes sure related regions are placed on the same region servers.
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();

		List<ServerName> allUnavailServers = minus(regions.values(), servers);
		Map<String, List<ServerName>> allAvailClusteredServers = clusterServers(servers);
		Collection<List<HRegionInfo>> allClusteredRegionGroups = clusterRegions(regions
				.keySet());

		for (List<HRegionInfo> clusteredRegionGroup : allClusteredRegionGroups) {
			Map<HRegionInfo, ServerName> clusterdRegionAndServerNameMap = getMapEntriesForKeys(
					regions, clusteredRegionGroup);

			Map<ServerName, List<HRegionInfo>> partialResult = retainAssignmentCluster(
					clusterdRegionAndServerNameMap, allAvailClusteredServers,
					allUnavailServers);

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
		return result;
	}

	private Map<ServerName, List<HRegionInfo>> retainAssignmentCluster(
			Map<HRegionInfo, ServerName> clusteredRegionAndServerNameMap,
			Map<String, List<ServerName>> allAvailClusteredServers,
			List<ServerName> allUnavailServers) {
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
		ArrayListMultimap<ServerName, HRegionInfo> localServerNameAndClusteredRegions = reverseMap(clusteredRegionAndServerNameMap);

		List<ServerName> localServers = new ArrayList<ServerName>(
				clusteredRegionAndServerNameMap.values());
		List<ServerName> localUnavailServers = intersect(allUnavailServers,
				localServers);
		List<ServerName> localAvailServers = minus(localServers,
				localUnavailServers);
		List<HRegionInfo> localUnplacedRegions = findUnplacedRegions(
				localServerNameAndClusteredRegions, localUnavailServers);

		for (ServerName unavailServer : localUnavailServers)
			localServerNameAndClusteredRegions.removeAll(unavailServer);

		List<List<ServerName>> bestPlacementRegionClusters;
		if (localAvailServers.size() == 0)
			bestPlacementRegionClusters = new ArrayList<List<ServerName>>(
					allAvailClusteredServers.values());
		else {
			String serverWithMajorityRegions = findServerWithMajorityRegions(localServerNameAndClusteredRegions);
			bestPlacementRegionClusters = new ArrayList<List<ServerName>>();
			bestPlacementRegionClusters.add(allAvailClusteredServers
					.get(serverWithMajorityRegions));

			for (ServerName server : localServerNameAndClusteredRegions
					.keySet()) {
				if (!server.getHostname().equals(serverWithMajorityRegions))
					localUnplacedRegions
							.addAll(localServerNameAndClusteredRegions
									.get(server));
			}
		}

		Map<HRegionInfo, ServerName> assignment = immediateAssignmentCluster(
				localUnplacedRegions, bestPlacementRegionClusters);
		for (Map.Entry<HRegionInfo, ServerName> entry : assignment.entrySet()) {
			if (!result.containsKey(entry.getValue()))
				result.put(entry.getValue(), new ArrayList<HRegionInfo>());
			result.get(entry.getValue()).add(entry.getKey());
		}
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
	private String findServerWithMajorityRegions(
			ArrayListMultimap<ServerName, HRegionInfo> serverNameAndRegionsMap) {
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

	private List<HRegionInfo> findUnplacedRegions(
			ArrayListMultimap<ServerName, HRegionInfo> regionsAndServers,
			List<ServerName> unavailableServers) {
		List<HRegionInfo> unplacedRegions = new ArrayList<HRegionInfo>();
		for (ServerName serverName : unavailableServers)
			if (regionsAndServers.containsKey(serverName))
				unplacedRegions.addAll(regionsAndServers.get(serverName));
		return unplacedRegions;
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

	private static interface ClusterDataKeyGenerator<V, K> {
		K generateKey(V v);
	}

	/**
	 * Groups the servers based on the hostname. If multiple region servers are
	 * running on the same host, then this will group them into a group.
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

	private static <K, V> ArrayListMultimap<V, K> reverseMap(Map<K, V> input) {
		ArrayListMultimap<V, K> multiMap = ArrayListMultimap.create();
		for (Map.Entry<K, V> entry : input.entrySet()) {
			multiMap.put(entry.getValue(), entry.getKey());
		}

		return multiMap;
	}
}
