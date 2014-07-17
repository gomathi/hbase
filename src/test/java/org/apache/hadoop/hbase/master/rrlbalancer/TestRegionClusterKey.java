package org.apache.hadoop.hbase.master.rrlbalancer;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestRegionClusterKey {

	@Test
	public void testRegionClusterKey() {
		RegionClusterKey rcKey = new RegionClusterKey("test",
				Bytes.toBytes("a"), Bytes.toBytes("z"));
		RegionClusterKey rcKeyTest = new RegionClusterKey("test",
				Bytes.toBytes("a"), Bytes.toBytes("z"));

		RegionClusterKey rcKeyTestAno = new RegionClusterKey("testOne",
				Bytes.toBytes("a"), Bytes.toBytes("z"));

		Assert.assertEquals(rcKey, rcKeyTest);
		Assert.assertEquals(rcKey.hashCode(), rcKeyTest.hashCode());

		Assert.assertFalse(rcKey.equals(rcKeyTestAno));
	}

}
