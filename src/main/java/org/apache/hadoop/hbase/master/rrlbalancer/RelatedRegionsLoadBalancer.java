package org.apache.hadoop.hbase.master.rrlbalancer;

import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.cluster;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.getMapEntriesForKeys;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.getValuesAsList;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.intersect;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.minus;
import static org.apache.hadoop.hbase.master.rrlbalancer.Utils.reverseMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
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

public class RelatedRegionsLoadBalancer implements LoadBalancer {
	private static final Log LOG = LogFactory.getLog(LoadBalancer.class);
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
	 * A modified version of the DefaultLoadBalancer#balanceCluster code to
	 * accomodate our need.
	 * 
	 * @param clusterState
	 * @return
	 */
	private Map<HRegionInfo, RegionPlan> balanceClusterToAverage(
			Map<ServerName, List<HRegionInfo>> clusterState) {
		long startTime = System.currentTimeMillis();

		int numServers = clusterState.size();
		if (numServers == 0) {
			LOG.debug("numServers=0 so skipping load balancing");
			return null;
		}
		NavigableMap<ServerAndLoad, List<List<HRegionInfo>>> serversByLoad = new TreeMap<ServerAndLoad, List<List<HRegionInfo>>>();
		int numRegions = 0;
		int maxRegionCountPerServer = 0;
		// Iterate so we can count regions as we build the map
		for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState
				.entrySet()) {
			List<HRegionInfo> regions = entry.getValue();
			List<List<HRegionInfo>> clusteredRegions = getValuesAsList(clusterRegions(regions));
			numRegions += clusteredRegions.size();

			if (maxRegionCountPerServer < clusteredRegions.size())
				maxRegionCountPerServer = clusteredRegions.size();

			serversByLoad.put(new ServerAndLoad(entry.getKey(),
					clusteredRegions.size()), clusteredRegions);
		}
		// Check if we even need to do any load balancing
		float average = (float) numRegions / numServers; // for logging
		// HBASE-3681 check sloppiness first
		int floor = (int) Math.floor(average * (1 - slop));
		int ceiling = (int) Math.ceil(average * (1 + slop));
		if ((serversByLoad.lastKey().getLoad() <= ceiling && serversByLoad
				.firstKey().getLoad() >= floor) || maxRegionCountPerServer == 1) {
			// Skipped because no server outside (min,max) range
			LOG.info("Skipping load balancing because balanced cluster; "
					+ "servers=" + numServers + " " + "regions=" + numRegions
					+ " average=" + average + " " + "mostloaded="
					+ serversByLoad.lastKey().getLoad() + " leastloaded="
					+ serversByLoad.firstKey().getLoad());
			return null;
		}
		int min = numRegions / numServers;
		int max = numRegions % numServers == 0 ? min : min + 1;

		// Using to check balance result.
		StringBuilder strBalanceParam = new StringBuilder();
		strBalanceParam.append("Balance parameter: numRegions=")
				.append(numRegions).append(", numServers=").append(numServers)
				.append(", max=").append(max).append(", min=").append(min);
		LOG.debug(strBalanceParam.toString());

		// Balance the cluster
		// TODO: Look at data block locality or a more complex load to do this
		Queue<RegionPlan> regionsToMove = new ArrayDeque<RegionPlan>();
		Map<HRegionInfo, RegionPlan> regionsToReturn = new HashMap<HRegionInfo, RegionPlan>();

		// Walk down most loaded, pruning each to the max
		int serversOverloaded = 0;
		Map<ServerName, BalanceInfo> serverBalanceInfo = new TreeMap<ServerName, BalanceInfo>();
		for (Map.Entry<ServerAndLoad, List<List<HRegionInfo>>> server : serversByLoad
				.descendingMap().entrySet()) {
			ServerAndLoad sal = server.getKey();
			int regionCount = sal.getLoad();
			if (regionCount <= max) {
				serverBalanceInfo.put(sal.getServerName(),
						new BalanceInfo(0, 0));
				break;
			}
			serversOverloaded++;
			List<List<HRegionInfo>> regions = server.getValue();
			int numToOffload = Math.min(regionCount - max, regions.size());

			int numTaken = 0;
			for (int i = 0; i <= numToOffload;) {
				List<HRegionInfo> hriCluster = regions.get(i); // fetch from
																// head
				i++;
				// Don't rebalance meta regions.
				if (isMetaRegion(hriCluster))
					continue;
				for (HRegionInfo hri : hriCluster)
					regionsToMove.add(new RegionPlan(hri, sal.getServerName(),
							null));
				numTaken++;
				if (numTaken >= numToOffload)
					break;
			}
			serverBalanceInfo.put(sal.getServerName(), new BalanceInfo(
					numToOffload, (-1) * numTaken));
		}
		int totalNumMoved = regionsToMove.size();

		// Walk down least loaded, filling each to the min
		int neededRegions = 0; // number of regions needed to bring all up to
								// min

		Map<ServerName, Integer> underloadedServers = new HashMap<ServerName, Integer>();
		int maxToTake = numRegions - (int) average;
		for (Map.Entry<ServerAndLoad, List<List<HRegionInfo>>> server : serversByLoad
				.entrySet()) {
			if (maxToTake == 0)
				break; // no more to take
			int regionCount = server.getKey().getLoad();
			if (regionCount >= min && regionCount > 0) {
				continue; // look for other servers which haven't reached min
			}
			int regionsToPut = min - regionCount;
			if (regionsToPut == 0) {
				regionsToPut = 1;
				maxToTake--;
			}
			underloadedServers.put(server.getKey().getServerName(),
					regionsToPut);
		}
		// number of servers that get new regions
		int serversUnderloaded = underloadedServers.size();
		int incr = 1;
		List<ServerName> sns = Arrays.asList(underloadedServers.keySet()
				.toArray(new ServerName[serversUnderloaded]));
		Collections.shuffle(sns, RANDOM);
		while (regionsToMove.size() > 0) {
			int cnt = 0;
			int i = incr > 0 ? 0 : underloadedServers.size() - 1;
			for (; i >= 0 && i < underloadedServers.size(); i += incr) {
				if (regionsToMove.isEmpty())
					break;
				ServerName si = sns.get(i);
				int numToTake = underloadedServers.get(si);
				if (numToTake == 0)
					continue;

				addRegionPlan(regionsToMove, si, regionsToReturn);

				underloadedServers.put(si, numToTake - 1);
				cnt++;
				BalanceInfo bi = serverBalanceInfo.get(si);
				if (bi == null) {
					bi = new BalanceInfo(0, 0);
					serverBalanceInfo.put(si, bi);
				}
				bi.setNumRegionsAdded(bi.getNumRegionsAdded() + 1);
			}
			if (cnt == 0)
				break;
			// iterates underloadedServers in the other direction
			incr = -incr;
		}
		for (Integer i : underloadedServers.values()) {
			// If we still want to take some, increment needed
			neededRegions += i;
		}

		// If none needed to fill all to min and none left to drain all to max,
		// we are done
		if (neededRegions == 0 && regionsToMove.isEmpty()) {
			long endTime = System.currentTimeMillis();
			LOG.info("Calculated a load balance in " + (endTime - startTime)
					+ "ms. " + "Moving " + totalNumMoved + " regions off of "
					+ serversOverloaded + " overloaded servers onto "
					+ serversUnderloaded + " less loaded servers");
			return regionsToReturn;
		}

		// Need to do a second pass.
		// Either more regions to assign out or servers that are still
		// underloaded

		// If we need more to fill min, grab one from each most loaded until
		// enough
		if (neededRegions != 0) {
			// Walk down most loaded, grabbing one from each until we get enough
			for (Map.Entry<ServerAndLoad, List<List<HRegionInfo>>> server : serversByLoad
					.descendingMap().entrySet()) {
				BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey()
						.getServerName());
				int idx = balanceInfo == null ? 0 : balanceInfo
						.getNextRegionForUnload();
				if (idx >= server.getValue().size())
					break;
				List<HRegionInfo> regionCluster = server.getValue().get(idx);
				if (isMetaRegion(regionCluster))
					continue; // Don't move meta regions.
				for (HRegionInfo hregion : regionCluster)
					regionsToMove.add(new RegionPlan(hregion, server.getKey()
							.getServerName(), null));
				totalNumMoved++;
				if (--neededRegions == 0) {
					// No more regions needed, done shedding
					break;
				}
			}
		}

		// Now we have a set of regions that must be all assigned out
		// Assign each underloaded up to the min, then if leftovers, assign to
		// max

		// Walk down least loaded, assigning to each to fill up to min
		for (Map.Entry<ServerAndLoad, List<List<HRegionInfo>>> server : serversByLoad
				.entrySet()) {
			int regionCount = server.getKey().getLoad();
			if (regionCount >= min)
				break;
			BalanceInfo balanceInfo = serverBalanceInfo.get(server.getKey()
					.getServerName());
			if (balanceInfo != null) {
				regionCount += balanceInfo.getNumRegionsAdded();
			}
			if (regionCount >= min) {
				continue;
			}
			int numToTake = min - regionCount;
			int numTaken = 0;
			while (numTaken < numToTake && 0 < regionsToMove.size()) {
				addRegionPlan(regionsToMove, server.getKey().getServerName(),
						regionsToReturn);
				numTaken++;
			}
		}

		// If we still have regions to dish out, assign underloaded to max
		if (0 < regionsToMove.size()) {
			for (Map.Entry<ServerAndLoad, List<List<HRegionInfo>>> server : serversByLoad
					.entrySet()) {
				int regionCount = server.getKey().getLoad();
				if (regionCount >= max) {
					break;
				}
				addRegionPlan(regionsToMove, server.getKey().getServerName(),
						regionsToReturn);
				if (regionsToMove.isEmpty()) {
					break;
				}
			}
		}

		long endTime = System.currentTimeMillis();

		if (!regionsToMove.isEmpty() || neededRegions != 0) {
			// Emit data so can diagnose how balancer went astray.
			LOG.warn("regionsToMove=" + totalNumMoved + ", numServers="
					+ numServers + ", serversOverloaded=" + serversOverloaded
					+ ", serversUnderloaded=" + serversUnderloaded);
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<ServerName, List<HRegionInfo>> e : clusterState
					.entrySet()) {
				if (sb.length() > 0)
					sb.append(", ");
				sb.append(e.getKey().toString());
				sb.append(" ");
				sb.append(e.getValue().size());
			}
			LOG.warn("Input " + sb.toString());
		}

		// All done!
		LOG.info("Done. Calculated a load balance in " + (endTime - startTime)
				+ "ms. " + "Moving " + totalNumMoved + " regions off of "
				+ serversOverloaded + " overloaded servers onto "
				+ serversUnderloaded + " less loaded servers");

		return regionsToReturn;
	}

	private void addRegionPlan(final Queue<RegionPlan> regionsToMove,
			final ServerName sn, Map<HRegionInfo, RegionPlan> regionsToReturn) {
		RegionPlan rp = null;
		rp = regionsToMove.remove();
		rp.setDestination(sn);
		regionsToReturn.put(rp.getRegionInfo(), rp);
	}

	private boolean isMetaRegion(List<HRegionInfo> hriCluster) {
		return false;
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
			if (loadBalMovResult.containsKey(hri)) {
				RegionPlan combinedRegionPlan = new RegionPlan(hri, entry
						.getValue().getSource(), loadBalMovResult.get(hri)
						.getDestination());
				loadBalMovResult.put(hri, combinedRegionPlan);
			}
		}
		return new ArrayList<RegionPlan>(loadBalMovResult.values());
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

		if (regions == null || regions.isEmpty())
			return assignments;
		if (servers == null || servers.isEmpty())
			return assignments;

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
