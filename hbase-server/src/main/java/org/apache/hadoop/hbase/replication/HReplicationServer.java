/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationServerStatusProtos.ReplicationServerStatusService;

/**
 * HReplicationServer which is responsible to all replication stuff. It checks in with
 * the HMaster. There are many HReplicationServers in a single HBase deployment.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "deprecation"})
public class HReplicationServer extends HRegionServer {

  private static final Logger LOG = LoggerFactory.getLogger(HReplicationServer.class);

  /** Parameter name for what region server implementation to use. */
  public static final String REPLICATION_SERVER_IMPL = "hbase.replicationserver.impl";

  /** replication server process name */
  public static final String REPLICATIONSERVER = "replicationserver";

  // Stub to do region server status calls against the master.
  private volatile ReplicationServerStatusService.BlockingInterface statusStub;

  public HReplicationServer(final Configuration conf) throws IOException {
    super(conf);  // thread name
    TraceUtil.initTracer(conf);
    try {

    } catch (Throwable t) {
      // Make sure we log the exception. HReplicationServer is often started via reflection and the
      // cause of failed startup is lost.
      LOG.error("Failed construction ReplicationServer", t);
      throw t;
    }
  }

  @Override
  protected String getProcessName() {
    return REPLICATIONSERVER;
  }

  @Override
  protected String getMyEphemeralNodePath() {
    return ZNodePaths.joinZNode(this.zooKeeper.getZNodePaths().replicationServerZNode,
        getServerName().toString());
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   */
  static HReplicationServer constructReplicationServer(
      final Class<? extends HReplicationServer> replicationServerClass,
      final Configuration conf) {
    try {
      Constructor<? extends HReplicationServer> c =
          replicationServerClass.getConstructor(Configuration.class);
      return c.newInstance(conf);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "ReplicationServer: "
          + replicationServerClass.toString(), e);
    }
  }

  @Override
  public void run() {
    super.run();
  }

  @Override
  protected void registerWithMaster() throws IOException {
    // TODO: 2020/7/14 do nothing temporarily
    try {
      createMyEphemeralNode();
    } catch (Throwable e) {
      stop("Failed initialization");
      throw new IOException("Region server startup failed", e);
    }
    // This call sets up an initialized replication and WAL. Later we start it up.
    setupWALAndReplication();
    // In here we start up the replication Service. Above we initialized it. TODO. Reconcile.
    // or make sense of it.
    startReplicationService();

    // Wake up anyone waiting for this server to online
    synchronized (online) {
      online.set(true);
      online.notifyAll();
    }
  }

  @Override
  protected void tryRegionServerReport(long reportStartTime, long reportEndTime) throws IOException {
    // TODO: 2020/7/13 do nothing temporarily
  }

  public static void main(String[] args) {
    LOG.info("STARTING executorService " + HReplicationServer.class.getSimpleName());
    VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HReplicationServer> regionServerClass =
        (Class<? extends HReplicationServer>)
            conf.getClass(REPLICATION_SERVER_IMPL, HReplicationServer.class);

    new HReplicationServerCommandLine(regionServerClass).doMain(args);
  }
}
