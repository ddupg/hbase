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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerMetricsBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

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
public class BaseServerManager {

  private static final Logger LOG = LoggerFactory.getLogger(BaseServerManager.class);

  public static final String MAX_CLOCK_SKEW_MS = "hbase.master.maxclockskew";

  /** Map of registered servers to their current load */
  protected final ConcurrentNavigableMap<ServerName, ServerMetrics> onlineServers =
    new ConcurrentSkipListMap<>();

  protected final MasterServices master;

  protected final DeadServer deadservers = new DeadServer();

  private final long maxSkew;
  private final long warningSkew;

  /** Listeners that are called on server events. */
  protected List<ServerListener> listeners = new CopyOnWriteArrayList<>();

  /**
   * Constructor.
   */
  public BaseServerManager(final MasterServices master) {
    this.master = master;
    Configuration c = master.getConfiguration();
    maxSkew = c.getLong(MAX_CLOCK_SKEW_MS, 30000);
    warningSkew = c.getLong("hbase.master.warningclockskew", 10000);
  }

  /**
   * Add the listener to the notification list.
   * @param listener The ServerListener to register
   */
  public void registerListener(final ServerListener listener) {
    this.listeners.add(listener);
  }

  /**
   * Remove the listener from the notification list.
   * @param listener The ServerListener to unregister
   */
  public boolean unregisterListener(final ServerListener listener) {
    return this.listeners.remove(listener);
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
    // Test for case where we get a region startup message from a regionserver
    // that has been quickly restarted but whose znode expiration handler has
    // not yet run, or from a server whose fail we are currently processing.
    // Test its host+port combo is present in serverAddressToServerInfo. If it
    // is, reject the server and trigger its expiration. The next time it comes
    // in, it should have been removed from serverAddressToServerInfo and queued
    // for processing by ProcessServerShutdown.

    final String hostname =
      request.hasUseThisHostnameInstead() ? request.getUseThisHostnameInstead() : ia.getHostName();
    ServerName sn = ServerName.valueOf(hostname, request.getPort(), request.getServerStartCode());
    checkClockSkew(sn, request.getServerCurrentTime());
    checkIsDead(sn, "STARTUP");
    if (!checkAndRecordNewServer(sn, ServerMetricsBuilder.of(sn, versionNumber, version))) {
      LOG.warn(
        "THIS SHOULD NOT HAPPEN, ServerStartup" + " could not record the server: " + sn);
    }
    return sn;
  }

  @VisibleForTesting
  public void serverReport(ServerName sn,
    ServerMetrics sl) throws YouAreDeadException {
    checkIsDead(sn, "REPORT");
    if (null == this.onlineServers.replace(sn, sl)) {
      // Already have this host+port combo and its just different start code?
      // Just let the server in. Presume master joining a running cluster.
      // recordNewServer is what happens at the end of reportServerStartup.
      // The only thing we are skipping is passing back to the regionserver
      // the ServerName to use. Here we presume a master has already done
      // that so we'll press on with whatever it gave us for ServerName.
      if (!checkAndRecordNewServer(sn, sl)) {
        LOG.info("RegionServerReport ignored, could not record the server: " + sn);
        return; // Not recorded, so no need to move on
      }
    }
  }

  /**
   * Check is a server of same host and port already exists,
   * if not, or the existed one got a smaller start code, record it.
   *
   * @param serverName the server to check and record
   * @param sl the server load on the server
   * @return true if the server is recorded, otherwise, false
   */
  boolean checkAndRecordNewServer(final ServerName serverName, final ServerMetrics sl) {
    ServerName existingServer = null;
    synchronized (this.onlineServers) {
      existingServer = findServerWithSameHostnamePortWithLock(serverName);
      if (existingServer != null && (existingServer.getStartcode() > serverName.getStartcode())) {
        LOG.info("Server serverName=" + serverName + " rejected; we already have "
            + existingServer.toString() + " registered with same hostname and port");
        return false;
      }
      recordNewServerWithLock(serverName, sl);
    }

    // Tell our listeners that a server was added
    if (!this.listeners.isEmpty()) {
      for (ServerListener listener : this.listeners) {
        listener.serverAdded(serverName);
      }
    }

    // Note that we assume that same ts means same server, and don't expire in that case.
    //  TODO: ts can theoretically collide due to clock shifts, so this is a bit hacky.
    if (existingServer != null &&
        (existingServer.getStartcode() < serverName.getStartcode())) {
      LOG.info("Triggering server recovery; existingServer " +
          existingServer + " looks stale, new server:" + serverName);
      expireServer(existingServer);
    }
    return true;
  }

  /**
   * Find out the region servers crashed between the crash of the previous master instance and the
   * current master instance and schedule SCP for them.
   * <p/>
   * Since the {@code RegionServerTracker} has already helped us to construct the online servers set
   * by scanning zookeeper, now we can compare the online servers with {@code liveServersFromWALDir}
   * to find out whether there are servers which are already dead.
   * <p/>
   * Must be called inside the initialization method of {@code RegionServerTracker} to avoid
   * concurrency issue.
   * @param deadServersFromPE the region servers which already have a SCP associated.
   * @param liveServersFromWALDir the live region servers from wal directory.
   */
  void findDeadServersAndProcess(Set<ServerName> deadServersFromPE,
      Set<ServerName> liveServersFromWALDir) {
    deadServersFromPE.forEach(deadservers::putIfAbsent);
    liveServersFromWALDir.stream().filter(sn -> !onlineServers.containsKey(sn))
      .forEach(this::expireServer);
  }

  /**
   * Checks if the clock skew between the server and the master. If the clock skew exceeds the
   * configured max, it will throw an exception; if it exceeds the configured warning threshold,
   * it will log a warning but start normally.
   * @param serverName Incoming servers's name
   * @param serverCurrentTime
   * @throws ClockOutOfSyncException if the skew exceeds the configured max value
   */
  protected void checkClockSkew(final ServerName serverName, final long serverCurrentTime)
      throws ClockOutOfSyncException {
    long skew = Math.abs(EnvironmentEdgeManager.currentTime() - serverCurrentTime);
    if (skew > maxSkew) {
      String message = "Server " + serverName + " has been " +
        "rejected; Reported time is too far out of sync with master.  " +
        "Time difference of " + skew + "ms > max allowed of " + maxSkew + "ms";
      LOG.warn(message);
      throw new ClockOutOfSyncException(message);
    } else if (skew > warningSkew){
      String message = "Reported time for server " + serverName + " is out of sync with master " +
        "by " + skew + "ms. (Warning threshold is " + warningSkew + "ms; " +
        "error threshold is " + maxSkew + "ms)";
      LOG.warn(message);
    }
  }

  /**
   * Called when RegionServer first reports in for duty and thereafter each
   * time it heartbeats to make sure it is has not been figured for dead.
   * If this server is on the dead list, reject it with a YouAreDeadException.
   * If it was dead but came back with a new start code, remove the old entry
   * from the dead list.
   * @param what START or REPORT
   */
  private void checkIsDead(final ServerName serverName, final String what)
      throws YouAreDeadException {
    if (this.deadservers.isDeadServer(serverName)) {
      // Exact match: host name, port and start code all match with existing one of the
      // dead servers. So, this server must be dead. Tell it to kill itself.
      String message = "Server " + what + " rejected; currently processing " +
          serverName + " as dead server";
      LOG.debug(message);
      throw new YouAreDeadException(message);
    }
    // Remove dead server with same hostname and port of newly checking in rs after master
    // initialization. See HBASE-5916 for more information.
    if ((this.master == null || this.master.isInitialized()) &&
        this.deadservers.cleanPreviousInstance(serverName)) {
      // This server has now become alive after we marked it as dead.
      // We removed it's previous entry from the dead list to reflect it.
      LOG.debug("{} {} came back up, removed it from the dead servers list", what, serverName);
    }
  }

  /**
   * Assumes onlineServers is locked.
   * @return ServerName with matching hostname and port.
   */
  private ServerName findServerWithSameHostnamePortWithLock(
      final ServerName serverName) {
    ServerName end = ServerName.valueOf(serverName.getHostname(), serverName.getPort(),
        Long.MAX_VALUE);

    ServerName r = onlineServers.lowerKey(end);
    if (r != null) {
      if (ServerName.isSameAddress(r, serverName)) {
        return r;
      }
    }
    return null;
  }

  /**
   * Adds the onlineServers list. onlineServers should be locked.
   * @param serverName The remote servers name.
   */
  @VisibleForTesting
  void recordNewServerWithLock(final ServerName serverName, final ServerMetrics sl) {
    LOG.info("Registering regionserver=" + serverName);
    this.onlineServers.put(serverName, sl);
  }

  /**
   * @param serverName
   * @return ServerMetrics if serverName is known else null
   */
  public ServerMetrics getLoad(final ServerName serverName) {
    return this.onlineServers.get(serverName);
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
    if (master.getServerManager().isClusterShutdown()) {
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
    return pid;
  }

  /**
   * Called when server has expired.
   */
  // Locking in this class needs cleanup.
  @VisibleForTesting
  public synchronized void moveFromOnlineToDeadServers(final ServerName sn) {
    synchronized (this.onlineServers) {
      boolean online = this.onlineServers.containsKey(sn);
      if (online) {
        // Remove the server from the known servers lists and update load info BUT
        // add to deadservers first; do this so it'll show in dead servers list if
        // not in online servers list.
        this.deadservers.putIfAbsent(sn);
        this.onlineServers.remove(sn);
        onlineServers.notifyAll();
      } else {
        // If not online, that is odd but may happen if 'Unknown Servers' -- where meta
        // has references to servers not online nor in dead servers list. If
        // 'Unknown Server', don't add to DeadServers else will be there for ever.
        LOG.trace("Expiration of {} but server not online", sn);
      }
    }
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    // TODO: optimize the load balancer call so we don't need to make a new list
    // TODO: FIX. THIS IS POPULAR CALL.
    return new ArrayList<>(this.onlineServers.keySet());
  }

  /**
   * @param keys The target server name
   * @param idleServerPredicator Evaluates the server on the given load
   * @return A copy of the internal list of online servers matched by the predicator
   */
  public List<ServerName> getOnlineServersListWithPredicator(List<ServerName> keys,
    Predicate<ServerMetrics> idleServerPredicator) {
    List<ServerName> names = new ArrayList<>();
    if (keys != null && idleServerPredicator != null) {
      keys.forEach(name -> {
        ServerMetrics load = onlineServers.get(name);
        if (load != null) {
          if (idleServerPredicator.test(load)) {
            names.add(name);
          }
        }
      });
    }
    return names;
  }

  public boolean isServerOnline(ServerName serverName) {
    return serverName != null && onlineServers.containsKey(serverName);
  }

  public enum ServerLiveState {
    LIVE,
    DEAD,
    UNKNOWN
  }

  /**
   * @return whether the server is online, dead, or unknown.
   */
  public synchronized ServerLiveState isServerKnownAndOnline(ServerName serverName) {
    return onlineServers.containsKey(serverName) ? ServerLiveState.LIVE
      : (deadservers.isDeadServer(serverName) ? ServerLiveState.DEAD : ServerLiveState.UNKNOWN);
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

  /**
   * Stop the ServerManager.
   */
  public void stop() {
  }

  /**
   * Creates a list of possible destinations for a region. It contains the online servers, but not
   *  the draining or dying servers.
   *  @param serversToExclude can be null if there is no server to exclude
   */
  public List<ServerName> createDestinationServersList(final List<ServerName> serversToExclude){
    final List<ServerName> destServers = getOnlineServersList();

    if (serversToExclude != null) {
      destServers.removeAll(serversToExclude);
    }

    return destServers;
  }

  /**
   * Calls {@link #createDestinationServersList} without server to exclude.
   */
  public List<ServerName> createDestinationServersList(){
    return createDestinationServersList(null);
  }

  /**
   * To clear any dead server with same host name and port of any online server
   */
  void clearDeadServersWithSameHostNameAndPortOfOnlineServer() {
    for (ServerName serverName : getOnlineServersList()) {
      deadservers.cleanAllPreviousInstances(serverName);
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
}
