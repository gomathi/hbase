package org.apache.hadoop.hbase.master.rrlbalancer;

import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.cluster;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.getMapEntriesForKeys;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.intersect;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.minus;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.reverseMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.apache.hadoop.hbase.master.DefaultLoadBalancer;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.rrlbalancer.Utils.ClusterDataKeyGenerator;

import com.google.common.base.Joiner;
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
	private static final TableClusterMapping TABLE_CLUSTER_MAPPING = new TableClusterMapping();

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
		Set<ServerNameAndClusteredRegions> sortedServerNamesAndClusteredRegions = new TreeSet<ServerNameAndClusteredRegions>();
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

}
