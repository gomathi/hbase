package org.apache.hadoop.hbase.master.rrlbalancer;

import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.cluster;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.getMapEntriesForKeys;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.getValuesAsList;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.intersect;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.minus;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.reverseMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
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
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.rrlbalancer.RegionsTruncatorIterator.TruncatedElement;
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

public class RelatedRegionsLoadBalancer implements LoadBalancer {
	private static final Log LOG = LogFactory
			.getLog(RelatedRegionsLoadBalancer.class);
	private static final Random RANDOM = new Random(System.currentTimeMillis());

	private final TableClusterMapping tableToClusterMapObj = new TableClusterMapping();
	private float slop;
	private Configuration conf;

	private final ClusterDataKeyGenerator<HRegionInfo, RegionClusterKey> regionKeyGener = new ClusterDataKeyGenerator<HRegionInfo, RegionClusterKey>() {

		@Override
		public RegionClusterKey generateKey(HRegionInfo hregionInfo) {
			// TODO Auto-generated method stub
			return new RegionClusterKey(
					tableToClusterMapObj.getClusterName(hregionInfo
							.getTableNameAsString()),
					hregionInfo.getStartKey(), hregionInfo.getEndKey());
		}

	};

	public RelatedRegionsLoadBalancer(List<Set<String>> clusteredTableNamesList) {
		if (clusteredTableNamesList != null)
			tableToClusterMapObj.addClusters(clusteredTableNamesList);
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		this.conf = conf;
		this.slop = conf.getFloat("hbase.regions.slop", (float) 0.2);
		if (slop < 0)
			slop = 0;
		else if (slop > 1)
			slop = 1;
	}

	@Override
	public void setClusterStatus(ClusterStatus st) {
		// TODO Auto-generated method stub
	}

	@Override
	public void setMasterServices(MasterServices masterServices) {
		// TODO Auto-generated method stub
	}

	@Override
	public List<RegionPlan> balanceCluster(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		// TODO Auto-generated method stub

		if (clusterState == null || clusterState.size() == 0) {
			LOG.debug("Empty cluster has been passed.");
			return null;
		}

		// We dont want to move around meta regions between region servers.
		for (ServerName serverName : clusterState.keySet()) {
			List<HRegionInfo> regions = clusterState.get(serverName);
			List<HRegionInfo> nonMetaRegions = removeMetaRegions(regions);
			clusterState.put(serverName, nonMetaRegions);
		}

		// First, lets move all the related regions to one region server, if the
		// related regions are fragmented across region servers.
		Map<HRegionInfo, RegionPlan> movRelRegionsResult = balanceClusterByMovingRelatedRegions(clusterState);

		// Lets balance related regions size across all the region servers.
		Map<HRegionInfo, RegionPlan> balanceRelRegionsResult = balanceClusterToAverage(clusterState);

		// Lets combine the result, and prepare a final region plans.
		return balanceClusterResult(movRelRegionsResult,
				balanceRelRegionsResult);
	}

	/**
	 * First step of {@link #balanceCluster(Map)}. If related regions are shared
	 * between two or more region servers, those related regions will be moved
	 * to one region server.
	 * 
	 * 
	 * Algorithm works as following
	 * 
	 * <ol>
	 * 
	 * <li>Input
	 * <li>Servers are S1, S2. Tables are T1, T2.
	 * <li>T1 contains regions [A,B]. T2 contains regions [X,Y].
	 * <li>A & X's key range -> [1..3], B & Y's key range -> [4..6].
	 * <li>S1 hosts [A,B] and S2 hosts [X,Y]
	 * 
	 * <li>Expected Output
	 * <li>S1 | S2 should host [A,X] and S1 | S2 should host [B,Y]
	 * 
	 * <li>Algorithm
	 * <li>Cluster all regions of each server, and put the clusters into a
	 * priority queue. In the priority queue, the clusters are sorted by
	 * {@link RegionClusterKey} and cluster size. Hence, if two region servers
	 * have parts of related regions, the parts will be returned as combined
	 * elements.
	 * 
	 * <li>Prepare the region plan for the region movements.
	 * 
	 * <ol>
	 * 
	 * 
	 * @param clusterState
	 * @return
	 */
	private Map<HRegionInfo, RegionPlan> balanceClusterByMovingRelatedRegions(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();
		PriorityQueue<ServerNameAndClusteredRegions> sortedQue = new PriorityQueue<ServerNameAndClusteredRegions>();
		for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
				.entrySet()) {
			ServerName serverName = entry.getKey();
			Map<RegionClusterKey, List<HRegionInfo>> clusteredRegions = clusterRegions(entry
					.getValue());
			for (Map.Entry<RegionClusterKey, List<HRegionInfo>> innerEntry : clusteredRegions
					.entrySet()) {
				sortedQue.add(new ServerNameAndClusteredRegions(serverName,
						innerEntry.getKey(), innerEntry.getValue()));
			}
		}

		Deque<ServerNameAndClusteredRegions> processingQue = new ArrayDeque<ServerNameAndClusteredRegions>();
		while (sortedQue.peek() != null) {

			while (sortedQue.peek() != null
					&& (processingQue.isEmpty() || processingQue.peekLast()
							.getRegionClusterKey()
							.equals(sortedQue.peek().getRegionClusterKey()))) {
				processingQue.addLast(sortedQue.remove());
			}

			ServerName dest = null;
			if (processingQue.size() > 1)
				dest = processingQue.peekLast().getServerName();
			while (processingQue.size() > 1) {
				ServerNameAndClusteredRegions temp = processingQue
						.removeFirst();
				for (HRegionInfo region : temp.getClusteredRegions()) {
					result.put(region,
							new RegionPlan(region, temp.getServerName(), dest));
				}
				clusterState.get(dest).addAll(temp.getClusteredRegions());
			}

			processingQue.clear();
		}
		return result;
	}

	/**
	 * Balances load across all the region servers.
	 * 
	 * Calculates the min and max of regions that could be handled by a region
	 * server.
	 * 
	 * If the value of regionssize / noofregionservers is an integer, then all
	 * servers will have same no of regions. Otherwise servers will
	 * 
	 * @param clusterState
	 * @return
	 */
	private Map<HRegionInfo, RegionPlan> balanceClusterToAverage(
			Map<ServerName, List<HRegionInfo>> clusterState) {

		Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();
		int maxRegions = 0;
		int numServers = clusterState.size();
		int numRegions = 0;

		NavigableSet<ServerAndLoad> serversByLoad = new TreeSet<ServerAndLoad>();
		for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
				.entrySet()) {
			List<List<HRegionInfo>> clusteredServerRegions = getValuesAsList(clusterRegions(entry
					.getValue()));
			numRegions += clusteredServerRegions.size();
			serversByLoad.add(new ServerAndLoad(entry.getKey(),
					clusteredServerRegions));

			if (maxRegions < clusteredServerRegions.size())
				maxRegions = clusteredServerRegions.size();
		}

		float avg = (float) numRegions / numServers;
		int floor = (int) Math.floor(avg * (1 - slop));
		int ceiling = (int) Math.ceil(avg * (1 + slop));

		if ((serversByLoad.last().getLoad() <= ceiling && serversByLoad.first()
				.getLoad() >= floor) || maxRegions == 1) {
			// Skipped because no server outside (min,max) range
			LOG.info("Skipping balanceClusterToAverage because balanced cluster; "
					+ "servers="
					+ numServers
					+ " "
					+ "regions="
					+ numRegions
					+ " average="
					+ avg
					+ " "
					+ "mostloaded="
					+ serversByLoad.last().getLoad()
					+ " leastloaded="
					+ serversByLoad.first().getLoad());
			return result;
		}

		int min = (numRegions / numServers);
		int max = (numRegions % numServers) == 0 ? min : min + 1;

		Map<HRegionInfo, RegionPlan> truncateServerByMaxResult = truncateRegionServersToMaxRegions(
				serversByLoad, min, max);
		truncateServerByMaxResult.putAll(balanceRegionServersToMinRegions(
				serversByLoad, min));

		return result;
	}

	/**
	 * Combines the result of {@link #balanceClusterByMovingRelatedRegions(Map)}
	 * and {@link #balanceClusterToAverage(Map)}
	 * 
	 * @param relRegionsMovResult
	 * @param loadBalMovResult
	 * @return
	 */
	private List<RegionPlan> balanceClusterResult(
			Map<HRegionInfo, RegionPlan> relRegionsMovResult,
			Map<HRegionInfo, RegionPlan> loadBalMovResult) {
		for (Map.Entry<HRegionInfo, RegionPlan> entry : relRegionsMovResult
				.entrySet()) {
			HRegionInfo hri = entry.getKey();
			RegionPlan rp = entry.getValue();
			if (loadBalMovResult.containsKey(hri)) {
				RegionPlan combinedRegionPlan = new RegionPlan(hri,
						rp.getSource(), loadBalMovResult.get(hri)
								.getDestination());
				loadBalMovResult.put(hri, combinedRegionPlan);
			} else
				loadBalMovResult.put(hri, rp);
		}
		return new ArrayList<RegionPlan>(loadBalMovResult.values());
	}

	private List<HRegionInfo> removeMetaRegions(List<HRegionInfo> regions) {
		List<HRegionInfo> result = new ArrayList<HRegionInfo>();
		for (HRegionInfo region : regions) {
			if (!region.isMetaRegion())
				result.add(region);
		}
		return result;
	}

	/**
	 * Makes sure no region servers are overloaded beyond 'max' regions.
	 * 
	 * This is done by removing regions from all region servers which have size
	 * > max. At the end all region servers will have size <= max.
	 * 
	 * @param serversByLoad
	 * @param min
	 * @param max
	 * @return
	 */
	private Map<HRegionInfo, RegionPlan> truncateRegionServersToMaxRegions(
			NavigableSet<ServerAndLoad> serversByLoad, int min, int max) {
		Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();

		RegionsTruncatorIterator maxTruncator = new RegionsTruncatorIterator(
				serversByLoad.descendingIterator(), max);

		ServersByLoadIterator serversByMinOrMaxLoadItr = new ServersByLoadIterator(
				serversByLoad.descendingIterator(), min);

		ServerAndLoad dest = null;
		int limit = min;
		while (maxTruncator.hasNext()) {
			if (!serversByMinOrMaxLoadItr.hasNext()) {
				serversByMinOrMaxLoadItr = new ServersByLoadIterator(
						serversByLoad.descendingIterator(), max);
				limit = max;
			}
			if (dest == null || dest.getLoad() > limit)
				dest = serversByMinOrMaxLoadItr.next();

			RegionsTruncatorIterator.TruncatedElement srcRegion = maxTruncator
					.next();
			dest.addCluster(srcRegion.regionsCluster);
			for (HRegionInfo hri : srcRegion.regionsCluster) {
				result.put(
						hri,
						new RegionPlan(hri, srcRegion.serverName, dest
								.getServerName()));
			}
		}

		return result;
	}

	private Map<HRegionInfo, RegionPlan> balanceRegionServersToMinRegions(
			NavigableSet<ServerAndLoad> serversByLoad, int min) {
		Map<HRegionInfo, RegionPlan> result = new HashMap<HRegionInfo, RegionPlan>();

		ServersByLoadIterator serversByMinLoadItr = new ServersByLoadIterator(
				serversByLoad.descendingIterator(), min);

		RegionsTruncatorIterator minTruncator = new RegionsTruncatorIterator(
				serversByLoad.descendingIterator(), min);

		ServerAndLoad dest = null;
		while ((serversByMinLoadItr.hasNext() || (dest != null && dest
				.getLoad() < min)) && minTruncator.hasNext()) {
			if (dest == null || dest.getLoad() >= min)
				dest = serversByMinLoadItr.next();

			TruncatedElement srcRegion = minTruncator.next();
			dest.addCluster(srcRegion.regionsCluster);
			for (HRegionInfo hri : srcRegion.regionsCluster) {
				result.put(
						hri,
						new RegionPlan(hri, srcRegion.serverName, dest
								.getServerName()));
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
		List<List<HRegionInfo>> clusteredRegions = getValuesAsList(clusterRegions(regions));
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
	 * Makes sure related regions are placed on the same region server.
	 * 
	 * The algorithms is as following
	 * 
	 * <ol>
	 * 
	 * <li>Cluster the regions based on (key range, and related tables group
	 * id).
	 * 
	 * <li>For each clustered region group, figure out whether any existing
	 * hosting region server of the region is dead, or figure out if regions are
	 * placed on different hosts. If yes, try to allocate a region server for
	 * the unplaced regions, and also move the existing regions to a region
	 * server where already the majority of the regions are living. This step is
	 * handled by internal method.
	 * 
	 * </ol>
	 */
	@Override
	public Map<ServerName, List<HRegionInfo>> retainAssignment(
			Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
		// TODO Auto-generated method stub
		Map<ServerName, List<HRegionInfo>> result = new TreeMap<ServerName, List<HRegionInfo>>();
		if (servers == null || servers.isEmpty())
			return result;

		int totReassignedCnt = 0;

		List<ServerName> allUnavailServers = minus(regions.values(), servers);
		Collection<List<HRegionInfo>> allClusteredRegionGroups = getValuesAsList(clusterRegions(regions
				.keySet()));

		for (List<HRegionInfo> clusteredRegionGroup : allClusteredRegionGroups) {
			Map<HRegionInfo, ServerName> localClusteredRegionAndServerNameMap = getMapEntriesForKeys(
					regions, clusteredRegionGroup);
			ListMultimap<ServerName, HRegionInfo> localServerNameAndClusteredRegions = reverseMap(localClusteredRegionAndServerNameMap);

			List<ServerName> localServers = new ArrayList<ServerName>(
					localClusteredRegionAndServerNameMap.values());
			List<ServerName> localUnavailServers = intersect(allUnavailServers,
					localServers);

			List<HRegionInfo> unavailableRegions = new ArrayList<HRegionInfo>();
			for (ServerName unavailServer : localUnavailServers) {
				unavailableRegions.addAll(localServerNameAndClusteredRegions
						.removeAll(unavailServer));
			}

			ServerName bestPlacementServer = (localServerNameAndClusteredRegions
					.size() == 0) ? (randomAssignment(servers))
					: findServerNameWithMajorityRegions(localServerNameAndClusteredRegions);

			for (ServerName serverName : localServerNameAndClusteredRegions
					.keySet()) {
				if (!bestPlacementServer.equals(serverName))
					unavailableRegions
							.addAll(localServerNameAndClusteredRegions
									.removeAll(serverName));
			}

			totReassignedCnt += unavailableRegions.size();
			if (!result.containsKey(bestPlacementServer))
				result.put(bestPlacementServer, new ArrayList<HRegionInfo>());
			result.get(bestPlacementServer).addAll(unavailableRegions);
			result.get(bestPlacementServer)
					.addAll(localServerNameAndClusteredRegions
							.get(bestPlacementServer));

		}
		LOG.info("No of unavailable servers which were previously assigned to regions : "
				+ allUnavailServers.size()
				+ "and unavailable hosts are"
				+ Joiner.on("\n").join(allUnavailServers));

		LOG.info("Total no of reassigned regions : " + totReassignedCnt);
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
	private ServerName findServerNameWithMajorityRegions(
			ListMultimap<ServerName, HRegionInfo> serverNameAndRegionsMap) {
		ServerName result = null;
		int maxRegionsCount = -1;
		for (ServerName serverName : serverNameAndRegionsMap.keySet()) {
			if (serverNameAndRegionsMap.get(serverName).size() > maxRegionsCount) {
				maxRegionsCount = serverNameAndRegionsMap.get(serverName)
						.size();
				result = serverName;
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
		Map<HRegionInfo, ServerName> assignments = new TreeMap<HRegionInfo, ServerName>();

		if (regions == null || regions.isEmpty()) {
			LOG.info("Empty regions have been passed. Returning empty assignments.");
			return assignments;
		}
		if (servers == null || servers.isEmpty()) {
			LOG.info("Empty servers have been passed. Returning empty assignments.");
			return assignments;
		}

		List<List<HRegionInfo>> clusteredRegionGroups = getValuesAsList(clusterRegions(regions));

		for (List<HRegionInfo> clusterRegionGroup : clusteredRegionGroups) {
			ServerName randomServer = randomAssignment(servers);
			for (HRegionInfo clusterRegion : clusterRegionGroup)
				assignments.put(clusterRegion, randomServer);
		}
		return assignments;
	}

	@Override
	public ServerName randomAssignment(List<ServerName> servers) {
		// TODO Auto-generated method stub
		if (servers == null || servers.isEmpty())
			return null;
		return servers.get(RANDOM.nextInt(servers.size()));
	}

	/**
	 * Groups the regions based on the region start key and region end key.
	 * 
	 * @param regions
	 * @return
	 */
	private Map<RegionClusterKey, List<HRegionInfo>> clusterRegions(
			Collection<HRegionInfo> regions) {
		return cluster(regions, regionKeyGener);
	}

}
