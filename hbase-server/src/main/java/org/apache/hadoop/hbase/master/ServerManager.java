/*
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
package org.apache.hadoop.hbase.master;

import static org.apache.hadoop.hbase.util.ConcurrentMapUtils.computeIfAbsent;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.AsyncRegionServerAdmin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos.StoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FlushedRegionSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FlushedSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.FlushedStoreSequenceId;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.com.google.protobuf.UnsafeByteOperations;

/**
 * The ServerManager class manages info about region servers.
 * <p>
 * Maintains lists of online and dead servers.  Processes the startups,
 * shutdowns, and deaths of region servers.
 * <p>
 * Servers are distinguished in two different ways.  A given server has a
 * location, specified by hostname and port, and of which there can only be one
 * online at any given time.  A server instance is specified by the location
 * (hostname and port) as well as the startcode (timestamp from when the server
 * was started).  This is used to differentiate a restarted instance of a given
 * server from the original instance.
 * <p>
 * If a sever is known not to be running any more, it is called dead. The dead
 * server needs to be handled by a ServerShutdownHandler.  If the handler is not
 * enabled yet, the server can't be handled right away so it is queued up.
 * After the handler is enabled, the server will be submitted to a handler to handle.
 * However, the handler may be just partially enabled.  If so,
 * the server cannot be fully processed, and be queued up for further processing.
 * A server is fully processed only after the handler is fully enabled
 * and has completed the handling.
 */
@InterfaceAudience.Private
public class ServerManager extends BaseServerManager {
  public static final String WAIT_ON_REGIONSERVERS_MAXTOSTART =
      "hbase.master.wait.on.regionservers.maxtostart";

  public static final String WAIT_ON_REGIONSERVERS_MINTOSTART =
      "hbase.master.wait.on.regionservers.mintostart";

  public static final String WAIT_ON_REGIONSERVERS_TIMEOUT =
      "hbase.master.wait.on.regionservers.timeout";

  public static final String WAIT_ON_REGIONSERVERS_INTERVAL =
      "hbase.master.wait.on.regionservers.interval";

  /**
   * see HBASE-20727
   * if set to true, flushedSequenceIdByRegion and storeFlushedSequenceIdsByRegion
   * will be persisted to HDFS and loaded when master restart to speed up log split
   */
  public static final String PERSIST_FLUSHEDSEQUENCEID =
      "hbase.master.persist.flushedsequenceid.enabled";

  public static final boolean PERSIST_FLUSHEDSEQUENCEID_DEFAULT = true;

  public static final String FLUSHEDSEQUENCEID_FLUSHER_INTERVAL =
      "hbase.master.flushedsequenceid.flusher.interval";

  public static final int FLUSHEDSEQUENCEID_FLUSHER_INTERVAL_DEFAULT =
      3 * 60 * 60 * 1000; // 3 hours

  private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);

  // Set if we are to shutdown the cluster.
  private AtomicBoolean clusterShutdown = new AtomicBoolean(false);

  /**
   * The last flushed sequence id for a region.
   */
  private final ConcurrentNavigableMap<byte[], Long> flushedSequenceIdByRegion =
    new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  private boolean persistFlushedSequenceId = true;
  private volatile boolean isFlushSeqIdPersistInProgress = false;
  /** File on hdfs to store last flushed sequence id of regions */
  private static final String LAST_FLUSHED_SEQ_ID_FILE = ".lastflushedseqids";
  private  FlushedSequenceIdFlusher flushedSeqIdFlusher;


  /**
   * The last flushed sequence id for a store in a region.
   */
  private final ConcurrentNavigableMap<byte[], ConcurrentNavigableMap<byte[], Long>>
    storeFlushedSequenceIdsByRegion = new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR);

  /** List of region servers that should not get any more new regions. */
  private final ArrayList<ServerName> drainingServers = new ArrayList<>();

  /**
   * Constructor.
   */
  public ServerManager(final MasterServices master) {
    super(master);
    Configuration c = master.getConfiguration();
    persistFlushedSequenceId = c.getBoolean(PERSIST_FLUSHEDSEQUENCEID,
        PERSIST_FLUSHEDSEQUENCEID_DEFAULT);
  }

  /**
   * Let the server manager know a new regionserver has come online
   * @param request the startup request
   * @param versionNumber the version number of the new regionserver
   * @param version the version of the new regionserver, could contain strings like "SNAPSHOT"
   * @param ia the InetAddress from which request is received
   * @return The ServerName we know this server as.
   * @throws IOException
   */
  ServerName serverStartup(RegionServerStartupRequest request, int versionNumber,
      String version, InetAddress ia) throws IOException {
    return super.serverStartup(request, versionNumber, version, ia);
  }

  /**
   * Updates last flushed sequence Ids for the regions on server sn
   * @param sn
   * @param hsl
   */
  private void updateLastFlushedSequenceIds(ServerName sn, ServerMetrics hsl) {
    for (Entry<byte[], RegionMetrics> entry : hsl.getRegionMetrics().entrySet()) {
      byte[] encodedRegionName = Bytes.toBytes(RegionInfo.encodeRegionName(entry.getKey()));
      Long existingValue = flushedSequenceIdByRegion.get(encodedRegionName);
      long l = entry.getValue().getCompletedSequenceId();
      // Don't let smaller sequence ids override greater sequence ids.
      if (LOG.isTraceEnabled()) {
        LOG.trace(Bytes.toString(encodedRegionName) + ", existingValue=" + existingValue +
          ", completeSequenceId=" + l);
      }
      if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue)) {
        flushedSequenceIdByRegion.put(encodedRegionName, l);
      } else if (l != HConstants.NO_SEQNUM && l < existingValue) {
        LOG.warn("RegionServer " + sn + " indicates a last flushed sequence id ("
            + l + ") that is less than the previous last flushed sequence id ("
            + existingValue + ") for region " + Bytes.toString(entry.getKey()) + " Ignoring.");
      }
      ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
          computeIfAbsent(storeFlushedSequenceIdsByRegion, encodedRegionName,
            () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
      for (Entry<byte[], Long> storeSeqId : entry.getValue().getStoreSequenceId().entrySet()) {
        byte[] family = storeSeqId.getKey();
        existingValue = storeFlushedSequenceId.get(family);
        l = storeSeqId.getValue();
        if (LOG.isTraceEnabled()) {
          LOG.trace(Bytes.toString(encodedRegionName) + ", family=" + Bytes.toString(family) +
            ", existingValue=" + existingValue + ", completeSequenceId=" + l);
        }
        // Don't let smaller sequence ids override greater sequence ids.
        if (existingValue == null || (l != HConstants.NO_SEQNUM && l > existingValue.longValue())) {
          storeFlushedSequenceId.put(family, l);
        }
      }
    }
  }

  @VisibleForTesting
  public void serverReport(ServerName sn, ServerMetrics sl) throws YouAreDeadException {
    super.serverReport(sn, sl);
    updateLastFlushedSequenceIds(sn, sl);
  }

  @VisibleForTesting
  public ConcurrentNavigableMap<byte[], Long> getFlushedSequenceIdByRegion() {
    return flushedSequenceIdByRegion;
  }

  public RegionStoreSequenceIds getLastFlushedSequenceId(byte[] encodedRegionName) {
    RegionStoreSequenceIds.Builder builder = RegionStoreSequenceIds.newBuilder();
    Long seqId = flushedSequenceIdByRegion.get(encodedRegionName);
    builder.setLastFlushedSequenceId(seqId != null ? seqId.longValue() : HConstants.NO_SEQNUM);
    Map<byte[], Long> storeFlushedSequenceId =
        storeFlushedSequenceIdsByRegion.get(encodedRegionName);
    if (storeFlushedSequenceId != null) {
      for (Map.Entry<byte[], Long> entry : storeFlushedSequenceId.entrySet()) {
        builder.addStoreSequenceId(StoreSequenceId.newBuilder()
            .setFamilyName(UnsafeByteOperations.unsafeWrap(entry.getKey()))
            .setSequenceId(entry.getValue().longValue()).build());
      }
    }
    return builder.build();
  }

  /**
   * Compute the average load across all region servers.
   * Currently, this uses a very naive computation - just uses the number of
   * regions being served, ignoring stats about number of requests.
   * @return the average load
   */
  public double getAverageLoad() {
    int totalLoad = 0;
    int numServers = 0;
    for (ServerMetrics sl : this.onlineServers.values()) {
      numServers++;
      totalLoad += sl.getRegionMetrics().size();
    }
    return numServers == 0 ? 0 :
      (double)totalLoad / (double)numServers;
  }

  /** @return the count of active regionservers */
  public int countOfRegionServers() {
    // Presumes onlineServers is a concurrent map
    return this.onlineServers.size();
  }

  /**
   * @return Read-only map of servers to serverinfo
   */
  public Map<ServerName, ServerMetrics> getOnlineServers() {
    // Presumption is that iterating the returned Map is OK.
    synchronized (this.onlineServers) {
      return Collections.unmodifiableMap(this.onlineServers);
    }
  }

  public DeadServer getDeadServers() {
    return this.deadservers;
  }

  /**
   * Checks if any dead servers are currently in progress.
   * @return true if any RS are being processed as dead, false if not
   */
  public boolean areDeadServersInProgress() {
    return this.deadservers.areDeadServersInProgress();
  }

  /**
   * Expire the passed server. Add it to list of dead servers and queue a shutdown processing.
   * @return pid if we queued a ServerCrashProcedure else {@link Procedure#NO_PROC_ID} if we did
   *         not (could happen for many reasons including the fact that its this server that is
   *         going down or we already have queued an SCP for this server or SCP processing is
   *         currently disabled because we are in startup phase).
   */
  @VisibleForTesting // Redo test so we can make this protected.
  public synchronized long expireServer(final ServerName serverName) {
    return expireServer(serverName, false);
  }

  synchronized long expireServer(final ServerName serverName, boolean force) {
    // THIS server is going down... can't handle our own expiration.
    if (serverName.equals(master.getServerName())) {
      if (!(master.isAborted() || master.isStopped())) {
        master.stop("We lost our znode?");
      }
      return Procedure.NO_PROC_ID;
    }
    if (this.deadservers.isDeadServer(serverName)) {
      LOG.warn("Expiration called on {} but already in DeadServer", serverName);
      return Procedure.NO_PROC_ID;
    }
    moveFromOnlineToDeadServers(serverName);

    // If server is in draining mode, remove corresponding znode
    // In some tests, the mocked HM may not have ZK Instance, hence null check
    if (master.getZooKeeper() != null) {
      String drainingZnode = ZNodePaths
        .joinZNode(master.getZooKeeper().getZNodePaths().drainingZNode, serverName.getServerName());
      try {
        ZKUtil.deleteNodeFailSilent(master.getZooKeeper(), drainingZnode);
      } catch (KeeperException e) {
        LOG.warn("Error deleting the draining znode for stopping server " + serverName.getServerName(), e);
      }
    }
    
    // If cluster is going down, yes, servers are going to be expiring; don't
    // process as a dead server
    if (isClusterShutdown()) {
      LOG.info("Cluster shutdown set; " + serverName +
        " expired; onlineServers=" + this.onlineServers.size());
      if (this.onlineServers.isEmpty()) {
        master.stop("Cluster shutdown set; onlineServer=0");
      }
      return Procedure.NO_PROC_ID;
    }
    LOG.info("Processing expiration of " + serverName + " on " + this.master.getServerName());
    long pid = master.getAssignmentManager().submitServerCrash(serverName, true, force);
    // Tell our listeners that a server was removed
    if (!this.listeners.isEmpty()) {
      this.listeners.stream().forEach(l -> l.serverRemoved(serverName));
    }
    // trigger a persist of flushedSeqId
    if (flushedSeqIdFlusher != null) {
      flushedSeqIdFlusher.triggerNow();
    }
    return pid;
  }

  /*
   * Remove the server from the drain list.
   */
  public synchronized boolean removeServerFromDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online.  ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. " +
               "Removing from draining list anyway, as requested.");
    }
    // Remove the server from the draining servers lists.
    return this.drainingServers.remove(sn);
  }

  /**
   * Add the server to the drain list.
   * @param sn
   * @return True if the server is added or the server is already on the drain list.
   */
  public synchronized boolean addServerToDrainList(final ServerName sn) {
    // Warn if the server (sn) is not online.  ServerName is of the form:
    // <hostname> , <port> , <startcode>

    if (!this.isServerOnline(sn)) {
      LOG.warn("Server " + sn + " is not currently online. " +
               "Ignoring request to add it to draining list.");
      return false;
    }
    // Add the server to the draining servers lists, if it's not already in
    // it.
    if (this.drainingServers.contains(sn)) {
      LOG.warn("Server " + sn + " is already in the draining server list." +
               "Ignoring request to add it again.");
      return true;
    }
    LOG.info("Server " + sn + " added to draining server list.");
    return this.drainingServers.add(sn);
  }

  /**
   * Contacts a region server and waits up to timeout ms
   * to close the region.  This bypasses the active hmaster.
   * Pass -1 as timeout if you do not want to wait on result.
   */
  public static void closeRegionSilentlyAndWait(AsyncClusterConnection connection,
      ServerName server, RegionInfo region, long timeout) throws IOException, InterruptedException {
    AsyncRegionServerAdmin admin = connection.getRegionServerAdmin(server);
    try {
      FutureUtils.get(
        admin.closeRegion(ProtobufUtil.buildCloseRegionRequest(server, region.getRegionName())));
    } catch (IOException e) {
      LOG.warn("Exception when closing region: " + region.getRegionNameAsString(), e);
    }
    if (timeout < 0) {
      return;
    }
    long expiration = timeout + System.currentTimeMillis();
    while (System.currentTimeMillis() < expiration) {
      try {
        RegionInfo rsRegion = ProtobufUtil.toRegionInfo(FutureUtils
          .get(
            admin.getRegionInfo(RequestConverter.buildGetRegionInfoRequest(region.getRegionName())))
          .getRegionInfo());
        if (rsRegion == null) {
          return;
        }
      } catch (IOException ioe) {
        if (ioe instanceof NotServingRegionException ||
          (ioe instanceof RemoteWithExtrasException &&
            ((RemoteWithExtrasException)ioe).unwrapRemoteException()
              instanceof NotServingRegionException)) {
          // no need to retry again
          return;
        }
        LOG.warn("Exception when retrieving regioninfo from: " + region.getRegionNameAsString(),
          ioe);
      }
      Thread.sleep(1000);
    }
    throw new IOException("Region " + region + " failed to close within" + " timeout " + timeout);
  }

  /**
   * Calculate min necessary to start. This is not an absolute. It is just
   * a friction that will cause us hang around a bit longer waiting on
   * RegionServers to check-in.
   */
  private int getMinToStart() {
    if (master.isInMaintenanceMode()) {
      // If in maintenance mode, then master hosting meta will be the only server available
      return 1;
    }

    int minimumRequired = 1;
    if (LoadBalancer.isTablesOnMaster(master.getConfiguration()) &&
        LoadBalancer.isSystemTablesOnlyOnMaster(master.getConfiguration())) {
      // If Master is carrying regions it will show up as a 'server', but is not handling user-
      // space regions, so we need a second server.
      minimumRequired = 2;
    }

    int minToStart = this.master.getConfiguration().getInt(WAIT_ON_REGIONSERVERS_MINTOSTART, -1);
    // Ensure we are never less than minimumRequired else stuff won't work.
    return Math.max(minToStart, minimumRequired);
  }

  /**
   * Wait for the region servers to report in.
   * We will wait until one of this condition is met:
   *  - the master is stopped
   *  - the 'hbase.master.wait.on.regionservers.maxtostart' number of
   *    region servers is reached
   *  - the 'hbase.master.wait.on.regionservers.mintostart' is reached AND
   *   there have been no new region server in for
   *      'hbase.master.wait.on.regionservers.interval' time AND
   *   the 'hbase.master.wait.on.regionservers.timeout' is reached
   *
   * @throws InterruptedException
   */
  public void waitForRegionServers(MonitoredTask status) throws InterruptedException {
    final long interval = this.master.getConfiguration().
        getLong(WAIT_ON_REGIONSERVERS_INTERVAL, 1500);
    final long timeout = this.master.getConfiguration().
        getLong(WAIT_ON_REGIONSERVERS_TIMEOUT, 4500);
    // Min is not an absolute; just a friction making us wait longer on server checkin.
    int minToStart = getMinToStart();
    int maxToStart = this.master.getConfiguration().
        getInt(WAIT_ON_REGIONSERVERS_MAXTOSTART, Integer.MAX_VALUE);
    if (maxToStart < minToStart) {
      LOG.warn(String.format("The value of '%s' (%d) is set less than '%s' (%d), ignoring.",
          WAIT_ON_REGIONSERVERS_MAXTOSTART, maxToStart,
          WAIT_ON_REGIONSERVERS_MINTOSTART, minToStart));
      maxToStart = Integer.MAX_VALUE;
    }

    long now =  System.currentTimeMillis();
    final long startTime = now;
    long slept = 0;
    long lastLogTime = 0;
    long lastCountChange = startTime;
    int count = countOfRegionServers();
    int oldCount = 0;
    // This while test is a little hard to read. We try to comment it in below but in essence:
    // Wait if Master is not stopped and the number of regionservers that have checked-in is
    // less than the maxToStart. Both of these conditions will be true near universally.
    // Next, we will keep cycling if ANY of the following three conditions are true:
    // 1. The time since a regionserver registered is < interval (means servers are actively checking in).
    // 2. We are under the total timeout.
    // 3. The count of servers is < minimum.
    for (ServerListener listener: this.listeners) {
      listener.waiting();
    }
    while (!this.master.isStopped() && !isClusterShutdown() && count < maxToStart &&
        ((lastCountChange + interval) > now || timeout > slept || count < minToStart)) {
      // Log some info at every interval time or if there is a change
      if (oldCount != count || lastLogTime + interval < now) {
        lastLogTime = now;
        String msg =
            "Waiting on regionserver count=" + count + "; waited="+
                slept + "ms, expecting min=" + minToStart + " server(s), max="+ getStrForMax(maxToStart) +
                " server(s), " + "timeout=" + timeout + "ms, lastChange=" + (lastCountChange - now) + "ms";
        LOG.info(msg);
        status.setStatus(msg);
      }

      // We sleep for some time
      final long sleepTime = 50;
      Thread.sleep(sleepTime);
      now =  System.currentTimeMillis();
      slept = now - startTime;

      oldCount = count;
      count = countOfRegionServers();
      if (count != oldCount) {
        lastCountChange = now;
      }
    }
    // Did we exit the loop because cluster is going down?
    if (isClusterShutdown()) {
      this.master.stop("Cluster shutdown");
    }
    LOG.info("Finished waiting on RegionServer count=" + count + "; waited=" + slept + "ms," +
        " expected min=" + minToStart + " server(s), max=" +  getStrForMax(maxToStart) + " server(s),"+
        " master is "+ (this.master.isStopped() ? "stopped.": "running"));
  }

  private String getStrForMax(final int max) {
    return max == Integer.MAX_VALUE? "NO_LIMIT": Integer.toString(max);
  }

  /**
   * @return A copy of the internal list of draining servers.
   */
  public List<ServerName> getDrainingServersList() {
    return new ArrayList<>(this.drainingServers);
  }

  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && onlineServers.containsKey(serverName);
  }

  /**
   * Check if a server is known to be dead.  A server can be online,
   * or known to be dead, or unknown to this manager (i.e, not online,
   * not known to be dead either; it is simply not tracked by the
   * master any more, for example, a very old previous instance).
   */
  public synchronized boolean isServerDead(ServerName serverName) {
    return serverName == null || deadservers.isDeadServer(serverName);
  }

  public void shutdownCluster() {
    String statusStr = "Cluster shutdown requested of master=" + this.master.getServerName();
    LOG.info(statusStr);
    this.clusterShutdown.set(true);
    if (onlineServers.isEmpty()) {
      // we do not synchronize here so this may cause a double stop, but not a big deal
      master.stop("OnlineServer=0 right after cluster shutdown set");
    }
  }

  public boolean isClusterShutdown() {
    return this.clusterShutdown.get();
  }

  /**
   * start chore in ServerManager
   */
  public void startChore() {
    Configuration c = master.getConfiguration();
    if (persistFlushedSequenceId) {
      // when reach here, RegionStates should loaded, firstly, we call remove deleted regions
      removeDeletedRegionFromLoadedFlushedSequenceIds();
      int flushPeriod = c.getInt(FLUSHEDSEQUENCEID_FLUSHER_INTERVAL,
          FLUSHEDSEQUENCEID_FLUSHER_INTERVAL_DEFAULT);
      flushedSeqIdFlusher = new FlushedSequenceIdFlusher(
          "FlushedSequenceIdFlusher", flushPeriod);
      master.getChoreService().scheduleChore(flushedSeqIdFlusher);
    }
  }

  /**
   * Stop the ServerManager.
   */
  public void stop() {
    super.stop();
    if (flushedSeqIdFlusher != null) {
      flushedSeqIdFlusher.cancel();
    }
    if (persistFlushedSequenceId) {
      try {
        persistRegionLastFlushedSequenceIds();
      } catch (IOException e) {
        LOG.warn("Failed to persist last flushed sequence id of regions"
            + " to file system", e);
      }
    }
  }

  /**
   * Creates a list of possible destinations for a region. It contains the online servers, but not
   *  the draining or dying servers.
   *  @param serversToExclude can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(final List<ServerName> serversToExclude){
    final List<ServerName> destServers = super.createDestinationServersList(serversToExclude);

    // Loop through the draining server list and remove them from the server list
    final List<ServerName> drainingServersCopy = getDrainingServersList();
    destServers.removeAll(drainingServersCopy);

    return destServers;
  }

  /**
   * Called by delete table and similar to notify the ServerManager that a region was removed.
   */
  public void removeRegion(final RegionInfo regionInfo) {
    final byte[] encodedName = regionInfo.getEncodedNameAsBytes();
    storeFlushedSequenceIdsByRegion.remove(encodedName);
    flushedSequenceIdByRegion.remove(encodedName);
  }

  @VisibleForTesting
  public boolean isRegionInServerManagerStates(final RegionInfo hri) {
    final byte[] encodedName = hri.getEncodedNameAsBytes();
    return (storeFlushedSequenceIdsByRegion.containsKey(encodedName)
        || flushedSequenceIdByRegion.containsKey(encodedName));
  }

  /**
   * Called by delete table and similar to notify the ServerManager that a region was removed.
   */
  public void removeRegions(final List<RegionInfo> regions) {
    for (RegionInfo hri: regions) {
      removeRegion(hri);
    }
  }

  /**
   * May return 0 when server is not online.
   */
  public int getVersionNumber(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getVersionNumber() : 0;
  }

  /**
   * May return "0.0.0" when server is not online
   */
  public String getVersion(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getVersion() : "0.0.0";
  }

  public int getInfoPort(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.get(serverName);
    return serverMetrics != null ? serverMetrics.getInfoServerPort() : 0;
  }

  /**
   * Persist last flushed sequence id of each region to HDFS
   * @throws IOException if persit to HDFS fails
   */
  private void persistRegionLastFlushedSequenceIds() throws IOException {
    if (isFlushSeqIdPersistInProgress) {
      return;
    }
    isFlushSeqIdPersistInProgress = true;
    try {
      Configuration conf = master.getConfiguration();
      Path rootDir = CommonFSUtils.getRootDir(conf);
      Path lastFlushedSeqIdPath = new Path(rootDir, LAST_FLUSHED_SEQ_ID_FILE);
      FileSystem fs = FileSystem.get(conf);
      if (fs.exists(lastFlushedSeqIdPath)) {
        LOG.info("Rewriting .lastflushedseqids file at: "
            + lastFlushedSeqIdPath);
        if (!fs.delete(lastFlushedSeqIdPath, false)) {
          throw new IOException("Unable to remove existing "
              + lastFlushedSeqIdPath);
        }
      } else {
        LOG.info("Writing .lastflushedseqids file at: " + lastFlushedSeqIdPath);
      }
      FSDataOutputStream out = fs.create(lastFlushedSeqIdPath);
      FlushedSequenceId.Builder flushedSequenceIdBuilder =
          FlushedSequenceId.newBuilder();
      try {
        for (Entry<byte[], Long> entry : flushedSequenceIdByRegion.entrySet()) {
          FlushedRegionSequenceId.Builder flushedRegionSequenceIdBuilder =
              FlushedRegionSequenceId.newBuilder();
          flushedRegionSequenceIdBuilder.setRegionEncodedName(
              ByteString.copyFrom(entry.getKey()));
          flushedRegionSequenceIdBuilder.setSeqId(entry.getValue());
          ConcurrentNavigableMap<byte[], Long> storeSeqIds =
              storeFlushedSequenceIdsByRegion.get(entry.getKey());
          if (storeSeqIds != null) {
            for (Entry<byte[], Long> store : storeSeqIds.entrySet()) {
              FlushedStoreSequenceId.Builder flushedStoreSequenceIdBuilder =
                  FlushedStoreSequenceId.newBuilder();
              flushedStoreSequenceIdBuilder.setFamily(ByteString.copyFrom(store.getKey()));
              flushedStoreSequenceIdBuilder.setSeqId(store.getValue());
              flushedRegionSequenceIdBuilder.addStores(flushedStoreSequenceIdBuilder);
            }
          }
          flushedSequenceIdBuilder.addRegionSequenceId(flushedRegionSequenceIdBuilder);
        }
        flushedSequenceIdBuilder.build().writeDelimitedTo(out);
      } finally {
        if (out != null) {
          out.close();
        }
      }
    } finally {
      isFlushSeqIdPersistInProgress = false;
    }
  }

  /**
   * Load last flushed sequence id of each region from HDFS, if persisted
   */
  public void loadLastFlushedSequenceIds() throws IOException {
    if (!persistFlushedSequenceId) {
      return;
    }
    Configuration conf = master.getConfiguration();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path lastFlushedSeqIdPath = new Path(rootDir, LAST_FLUSHED_SEQ_ID_FILE);
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(lastFlushedSeqIdPath)) {
      LOG.info("No .lastflushedseqids found at" + lastFlushedSeqIdPath
          + " will record last flushed sequence id"
          + " for regions by regionserver report all over again");
      return;
    } else {
      LOG.info("begin to load .lastflushedseqids at " + lastFlushedSeqIdPath);
    }
    FSDataInputStream in = fs.open(lastFlushedSeqIdPath);
    try {
      FlushedSequenceId flushedSequenceId =
          FlushedSequenceId.parseDelimitedFrom(in);
      if (flushedSequenceId == null) {
        LOG.info(".lastflushedseqids found at {} is empty", lastFlushedSeqIdPath);
        return;
      }
      for (FlushedRegionSequenceId flushedRegionSequenceId : flushedSequenceId
          .getRegionSequenceIdList()) {
        byte[] encodedRegionName = flushedRegionSequenceId
            .getRegionEncodedName().toByteArray();
        flushedSequenceIdByRegion
            .putIfAbsent(encodedRegionName, flushedRegionSequenceId.getSeqId());
        if (flushedRegionSequenceId.getStoresList() != null
            && flushedRegionSequenceId.getStoresList().size() != 0) {
          ConcurrentNavigableMap<byte[], Long> storeFlushedSequenceId =
              computeIfAbsent(storeFlushedSequenceIdsByRegion, encodedRegionName,
                () -> new ConcurrentSkipListMap<>(Bytes.BYTES_COMPARATOR));
          for (FlushedStoreSequenceId flushedStoreSequenceId : flushedRegionSequenceId
              .getStoresList()) {
            storeFlushedSequenceId
                .put(flushedStoreSequenceId.getFamily().toByteArray(),
                    flushedStoreSequenceId.getSeqId());
          }
        }
      }
    } finally {
      in.close();
    }
  }

  /**
   * Regions may have been removed between latest persist of FlushedSequenceIds
   * and master abort. So after loading FlushedSequenceIds from file, and after
   * meta loaded, we need to remove the deleted region according to RegionStates.
   */
  public void removeDeletedRegionFromLoadedFlushedSequenceIds() {
    RegionStates regionStates = master.getAssignmentManager().getRegionStates();
    Iterator<byte[]> it = flushedSequenceIdByRegion.keySet().iterator();
    while(it.hasNext()) {
      byte[] regionEncodedName = it.next();
      if (regionStates.getRegionState(Bytes.toStringBinary(regionEncodedName)) == null) {
        it.remove();
        storeFlushedSequenceIdsByRegion.remove(regionEncodedName);
      }
    }
  }

  private class FlushedSequenceIdFlusher extends ScheduledChore {

    public FlushedSequenceIdFlusher(String name, int p) {
      super(name, master, p, 60 * 1000); //delay one minute before first execute
    }

    @Override
    protected void chore() {
      try {
        persistRegionLastFlushedSequenceIds();
      } catch (IOException e) {
        LOG.debug("Failed to persist last flushed sequence id of regions"
            + " to file system", e);
      }
    }
  }
}
