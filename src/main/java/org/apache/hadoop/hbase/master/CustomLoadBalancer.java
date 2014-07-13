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
	 * This is called during the cluster initialization. It is critical to
	 * provide the correct placement logic.
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();

		List<ServerName> allUnavailServers = minus(regions.values(), servers);
		Map<String, List<ServerName>> allAvailClusteredServers = clusterServers(servers);
		Collection<List<HRegionInfo>> clusteredRegionGroups = clusterRegions(regions
				.keySet());

		for (List<HRegionInfo> clusteredRegionGroup : clusteredRegionGroups) {
			Map<HRegionInfo, ServerName> regionAndServerNameMap = getMapEntriesForKeys(
					regions, clusteredRegionGroup);

			Map<ServerName, List<HRegionInfo>> partialResult = retainAssignmentCluster(
					regionAndServerNameMap, allAvailClusteredServers,
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
		return result;
	}

	private Map<ServerName, List<HRegionInfo>> retainAssignmentCluster(
			Map<HRegionInfo, ServerName> regions,
			Map<String, List<ServerName>> allAvailClusteredServers,
			List<ServerName> allUnavailServers) {
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
		ArrayListMultimap<ServerName, HRegionInfo> serverNameAndRegionsMap = reverseMap(regions);

		List<ServerName> servers = new ArrayList<ServerName>(
				removeDuplicates(regions.values()));
		List<ServerName> unavailServers = intersect(allUnavailServers, servers);
		List<ServerName> availServers = minus(servers, unavailServers);
		List<HRegionInfo> unplacedRegions = findUnplacedRegions(
				serverNameAndRegionsMap, unavailServers);

		for (ServerName unavailServer : unavailServers)
			serverNameAndRegionsMap.removeAll(unavailServer);

		List<List<ServerName>> bestPlacementRegionClusters;
		if (availServers.size() == 0)
			bestPlacementRegionClusters = new ArrayList<List<ServerName>>(
					allAvailClusteredServers.values());
		else {
			ServerName serverWithMajorityRegions = findServerNameWithMajorityRegions(serverNameAndRegionsMap);
			bestPlacementRegionClusters = new ArrayList<List<ServerName>>();
			bestPlacementRegionClusters.add(allAvailClusteredServers
					.get(serverWithMajorityRegions.getHostname()));

			for (ServerName server : serverNameAndRegionsMap.keySet()) {
				if (!server.getHostname().equals(
						serverWithMajorityRegions.getHostname()))
					unplacedRegions.addAll(serverNameAndRegionsMap.get(server));
			}
		}

		Map<HRegionInfo, ServerName> assignment = immediateAssignmentCluster(
				unplacedRegions, bestPlacementRegionClusters);
		for (Map.Entry<HRegionInfo, ServerName> entry : assignment.entrySet()) {
			if (!result.containsKey(entry.getValue()))
				result.put(entry.getValue(), new ArrayList<HRegionInfo>());
			result.get(entry.getValue()).add(entry.getKey());
		}
		return result;
	}

	private ServerName findServerNameWithMajorityRegions(
			ArrayListMultimap<ServerName, HRegionInfo> serverNameAndRegionsMap) {
		ServerName result = null;
		int maxRegionsSize = -1;
		for (ServerName serverName : serverNameAndRegionsMap.keySet()) {
			if (maxRegionsSize < serverNameAndRegionsMap.get(serverName).size()) {
				maxRegionsSize = serverNameAndRegionsMap.size();
				result = serverName;
			}
		}

		return result;
	}

	private List<HRegionInfo> findUnplacedRegions(
			ArrayListMultimap<ServerName, HRegionInfo> regionsAndServers,
			List<ServerName> unavailableServers) {
		List<HRegionInfo> hRegions = new ArrayList<HRegionInfo>();
		for (ServerName serverName : unavailableServers)
			if (regionsAndServers.containsKey(serverName))
				hRegions.addAll(regionsAndServers.get(serverName));
		return hRegions;
	}

	private static <K, V> ArrayListMultimap<V, K> reverseMap(Map<K, V> input) {
		ArrayListMultimap<V, K> multiMap = ArrayListMultimap.create();
		for (Map.Entry<K, V> entry : input.entrySet()) {
			multiMap.put(entry.getValue(), entry.getKey());
		}

		return multiMap;
	}

	private static <A> Collection<A> removeDuplicates(Collection<A> input) {
		Set<A> set = new HashSet<A>();
		set.addAll(input);
		input.clear();
		input.addAll(set);
		return input;
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
}
