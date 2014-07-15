package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Given a table name, gives the corresponding cluster name. The user has to
 * initialize with the related tables information.
 * 
 */

class TableClusterMapping {

	private static final Random RANDOM = new Random(System.currentTimeMillis());
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