package org.apache.hadoop.hbase.master;

import java.io.File;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.Assert;
import org.junit.Test;

public class TestIncludeExcludeHosts {

  @Test
  public void testExcludeHost() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    HBaseTestingUtility obj = new HBaseTestingUtility(conf);
    String excludeFile = obj.getDataTestDir() + Path.SEPARATOR + "exclude.conf";

    conf.set("hbase.master.includeexcluderegionservers", excludeFile);

    obj.startMiniCluster(1, 1);

    MiniHBaseCluster cluster = obj.getMiniHBaseCluster();

    RegionServerThread regionThread = cluster.startRegionServer();
    ServerName serverName = regionThread.getRegionServer().getServerName();

    Assert.assertTrue(cluster.getMaster().getServerManager().getOnlineServersList()
        .contains(serverName));

    PrintWriter writer = new PrintWriter(new File(excludeFile));
    writer.println(serverName.getHostname() + "\\:" + serverName.getPort() + " = deny");
    writer.close();

    cluster.getMaster().getServerManager().refreshIncludeExcludeRSConfig();

    Thread.sleep(6000);

    Assert.assertFalse(cluster.getMaster().getServerManager().getOnlineServersList()
        .contains(serverName));
  }
}
