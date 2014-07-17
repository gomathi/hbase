package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TestServerNameAndClusteredRegions {
	private final static Random RANDOM = new Random(System.currentTimeMillis());

	@Test
	public void testForCorrectness() {
		RegionClusterKey rcKey = new RegionClusterKey("d", Bytes.toBytes("d"),
				Bytes.toBytes("f"));
		ServerNameAndClusteredRegions snacr = new ServerNameAndClusteredRegions(
				new ServerName("localhost", RANDOM.nextInt(), RANDOM.nextLong()),
				rcKey, generateHRegionsList(3));

		ServerNameAndClusteredRegions snacrAno = new ServerNameAndClusteredRegions(
				new ServerName("localhost", RANDOM.nextInt(), RANDOM.nextLong()),
				rcKey, generateHRegionsList(4));

		PriorityQueue<ServerNameAndClusteredRegions> testSet = new PriorityQueue<ServerNameAndClusteredRegions>(
				10,
				new ServerNameAndClusteredRegions.ServerNameAndClusteredRegionsComparator());
		testSet.add(snacrAno);
		testSet.add(snacr);

		Assert.assertEquals(testSet.remove(), snacr);
		Assert.assertEquals(testSet.remove(), snacrAno);

		RegionClusterKey rcKeyAno = new RegionClusterKey("a",
				Bytes.toBytes("a"), Bytes.toBytes("c"));

		ServerNameAndClusteredRegions snacrThird = new ServerNameAndClusteredRegions(
				new ServerName("localhost", RANDOM.nextInt(), RANDOM.nextLong()),
				rcKeyAno, generateHRegionsList(4));
		testSet.add(snacrAno);
		testSet.add(snacr);
		testSet.add(snacrThird);

		Assert.assertEquals(testSet.remove(), snacrThird);
		Assert.assertEquals(testSet.remove(), snacr);
		Assert.assertEquals(testSet.remove(), snacrAno);
	}

	private List<HRegionInfo> generateHRegionsList(int count) {
		List<HRegionInfo> hRegions = new ArrayList<HRegionInfo>();
		for (int i = 0; i < count; i++)
			hRegions.add(new HRegionInfo());
		return hRegions;
	}
}
