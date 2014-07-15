package org.apache.hadoop.hbase.master.rrlbalancer;

/**
 * A holder class to count total no of reassigned regions during
 * {@link RelatedRegionsLoadBalancer#retainAssignment(Map, List)}
 * 
 */
interface ReassignedRegionsCounter {
	void incrCounterBy(int value);

	int getCurrCount();
}