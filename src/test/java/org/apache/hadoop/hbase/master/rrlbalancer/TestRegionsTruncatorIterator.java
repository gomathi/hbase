package org.apache.hadoop.hbase.master.rrlbalancer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.junit.Test;

public class TestRegionsTruncatorIterator {

	private static final Random RANDOM = new Random(System.currentTimeMillis());

	@Test
	public void testForCorrectness() {
		int[] loads = { 1, 2 };

		List<ServerAndLoad> list = generateServerAndLoad(loads);
		RegionsTruncatorIterator tItr = new RegionsTruncatorIterator(
				list.iterator(), 5);
		testIterator(tItr, 0);

		tItr = new RegionsTruncatorIterator(list.iterator(), 1);
		testIterator(tItr, 1);

		tItr = new RegionsTruncatorIterator(list.iterator(), 0);
		testIterator(tItr, 3);
	}

	private void testIterator(RegionsTruncatorIterator itr, int expectedCnt) {
		int testCnt = 0;
		while (itr.hasNext()) {
			itr.next();
			testCnt++;
		}

		Assert.assertEquals(expectedCnt, testCnt);
	}

	private List<ServerAndLoad> generateServerAndLoad(int[] loads) {
		List<ServerAndLoad> result = new ArrayList<ServerAndLoad>();
		for (int load : loads) {
			List<List<HRegionInfo>> clusteredRegions = new ArrayList<List<HRegionInfo>>();
			for (int i = 0; i < load; i++) {
				List<HRegionInfo> list = new ArrayList<HRegionInfo>();
				list.add(new HRegionInfo());
				clusteredRegions.add(list);
			}
			ServerAndLoad sal = new ServerAndLoad(
					new ServerName(Integer.toString(load), RANDOM.nextInt(),
							RANDOM.nextInt()), clusteredRegions);
			result.add(sal);
		}
		return result;
	}

}
