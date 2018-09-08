/*
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
package org.lealone.aose.server;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.aose.config.Config;
import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.ApplicationState;
import org.lealone.aose.gms.EndpointState;
import org.lealone.aose.gms.FailureDetector;
import org.lealone.aose.gms.Gossiper;
import org.lealone.aose.gms.IEndpointStateChangeSubscriber;
import org.lealone.aose.gms.VersionedValue;
import org.lealone.aose.gms.VersionedValue.VersionedValueFactory;
import org.lealone.aose.locator.IEndpointSnitch;
import org.lealone.aose.locator.TopologyMetaData;
import org.lealone.aose.net.MessagingService;
import org.lealone.aose.util.FileUtils;
import org.lealone.aose.util.Pair;
import org.lealone.aose.util.Utils;
import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.net.NetEndpoint;
import org.lealone.server.ProtocolServer;

import com.sun.management.OperatingSystemMXBean;

/**
 * This abstraction contains the token/identifier of this node
 * on the identifier space. This token gets gossiped around.
 * This class will also maintain histograms of the load information
 * of other nodes in the cluster.
 * 
 * @author Cassandra Group
 * @author zhh
 */
public class P2pServer implements IEndpointStateChangeSubscriber, ProtocolServer {

    private static final Logger logger = LoggerFactory.getLogger(P2pServer.class);
    private static final BackgroundActivityMonitor bgMonitor = new BackgroundActivityMonitor();

    public static final P2pServer instance = new P2pServer();
    public static final VersionedValueFactory valueFactory = new VersionedValueFactory();

    private final TopologyMetaData topologyMetaData = new TopologyMetaData();
    private final List<IEndpointLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    private boolean started;

    private Map<String, String> config;
    private String host = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_P2P_PORT;
    private boolean ssl;

    private String localHostId;
    private ServerEncryptionOptions options;

    private P2pServer() {
    }

    @Override
    public void init(Map<String, String> config) {
        this.config = config;
        if (config.containsKey("host"))
            host = config.get("host");
        if (config.containsKey("port"))
            port = Integer.parseInt(config.get("port"));

        ssl = Boolean.parseBoolean(config.get("ssl"));

        NetEndpoint.setLocalP2pEndpoint(host, port);
    }

    @Override
    public void stop() {
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
    }

    @Override
    public synchronized void start() throws ConfigException {
        if (started)
            return;
        started = true;

        loadPersistedNodeInfo();
        prepareToJoin();
        joinCluster();
    }

    private void loadPersistedNodeInfo() {
        if (Boolean.parseBoolean(Config.getProperty("load.persisted.node.info", "true"))) {
            logger.info("Loading persisted node info");
            Map<NetEndpoint, String> loadedHostIds = ClusterMetaData.loadHostIds();
            NetEndpoint local = ConfigDescriptor.getLocalEndpoint();
            for (NetEndpoint ep : loadedHostIds.keySet()) {
                if (!ep.equals(local)) {
                    topologyMetaData.updateHostId(loadedHostIds.get(ep), ep);
                    Gossiper.instance.addSavedEndpoint(ep);
                }
            }
        }
    }

    private void prepareToJoin() throws ConfigException {
        localHostId = NetEndpoint.getLocalTcpHostAndPort();
        topologyMetaData.updateHostId(localHostId, ConfigDescriptor.getLocalEndpoint());

        int generation = ClusterMetaData.incrementAndGetGeneration(localHostId);

        Map<ApplicationState, VersionedValue> appStates = new HashMap<>();
        appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId));
        appStates.put(ApplicationState.TCP_ENDPOINT, valueFactory.endpoint(NetEndpoint.getLocalTcpEndpoint()));
        appStates.put(ApplicationState.P2P_ENDPOINT, valueFactory.endpoint(NetEndpoint.getLocalP2pEndpoint()));
        appStates.put(ApplicationState.DC, getDatacenter());
        appStates.put(ApplicationState.RACK, getRack());
        appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
        appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());

        // 先启动MessagingService再启动Gossiper
        MessagingService.instance().start(ConfigDescriptor.getLocalEndpoint(), config);

        logger.info("Starting up server gossip");
        Gossiper.instance.register(this);
        Gossiper.instance.start(generation, appStates);

        logger.info("Starting up LoadBroadcaster");
        LoadBroadcaster.instance.startBroadcasting();
    }

    // gossip snitch infos (local DC and rack)
    public void gossipSnitchInfo() {
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC, getDatacenter());
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, getRack());
    }

    private VersionedValue getDatacenter() {
        IEndpointSnitch snitch = ConfigDescriptor.getEndpointSnitch();
        String dc = snitch.getDatacenter(ConfigDescriptor.getLocalEndpoint());
        return valueFactory.datacenter(dc);
    }

    private VersionedValue getRack() {
        IEndpointSnitch snitch = ConfigDescriptor.getEndpointSnitch();
        String rack = snitch.getRack(ConfigDescriptor.getLocalEndpoint());
        return valueFactory.rack(rack);
    }

    private void joinCluster() {
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>(1);
        states.add(Pair.create(ApplicationState.STATUS, valueFactory.normal(localHostId)));
        Gossiper.instance.addLocalApplicationStates(states);
    }

    public void register(IEndpointLifecycleSubscriber subscriber) {
        lifecycleSubscribers.add(subscriber);
    }

    public void unregister(IEndpointLifecycleSubscriber subscriber) {
        lifecycleSubscribers.remove(subscriber);
    }

    public void startGossiping() {
        if (!started) {
            logger.warn("Starting gossip by operator request");
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
            started = true;
        }
    }

    public void stopGossiping() {
        if (started) {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            started = false;
        }
    }

    public boolean isGossipRunning() {
        return Gossiper.instance.isEnabled();
    }

    public boolean isStarted() {
        return started;
    }

    public TopologyMetaData getTopologyMetaData() {
        return topologyMetaData;
    }

    /**
     * Increment about the known Compaction severity of the events in this node
     */
    public void reportSeverity(double incr) {
        bgMonitor.incrCompactionSeverity(incr);
    }

    public void reportManualSeverity(double incr) {
        bgMonitor.incrManualSeverity(incr);
    }

    public double getSeverity(NetEndpoint endpoint) {
        return bgMonitor.getSeverity(endpoint);
    }

    public String getLocalHostId() {
        return localHostId;
    }

    public String getLocalHostIdAsString() {
        return topologyMetaData.getHostId(ConfigDescriptor.getLocalEndpoint());
    }

    public Map<String, String> getHostIdMap() {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<NetEndpoint, String> entry : topologyMetaData.getEndpointToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(), entry.getValue().toString());
        return mapOut;
    }

    @Override
    public void beforeChange(NetEndpoint endpoint, EndpointState currentState, ApplicationState newStateKey,
            VersionedValue newValue) {
        // no-op
    }

    @Override
    public void onChange(NetEndpoint endpoint, ApplicationState state, VersionedValue value) {
        if (state == ApplicationState.STATUS) {
            String apStateValue = value.value;
            String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName) {
            case VersionedValue.STATUS_NORMAL:
                handleStateNormal(endpoint);
                break;
            case VersionedValue.REMOVING_TOKEN:
            case VersionedValue.REMOVED_TOKEN:
                handleStateRemoving(endpoint, pieces);
                break;
            case VersionedValue.STATUS_LEAVING:
                handleStateLeaving(endpoint);
                break;
            case VersionedValue.STATUS_LEFT:
                handleStateLeft(endpoint, pieces);
                break;
            }
        } else {
            EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
            if (epState == null || Gossiper.instance.isDeadState(epState)) {
                logger.debug("Ignoring state change for dead or unknown endpoint: {}", endpoint);
                return;
            }

            if (!endpoint.equals(ConfigDescriptor.getLocalEndpoint()))
                updatePeerInfo(endpoint, state, value);
        }
    }

    private void updatePeerInfo(NetEndpoint endpoint, ApplicationState state, VersionedValue value) {
        switch (state) {
        case HOST_ID:
            ClusterMetaData.updatePeerInfo(endpoint, "host_id", value.value);
            break;
        case TCP_ENDPOINT:
            ClusterMetaData.updatePeerInfo(endpoint, "tcp_endpoint", value.value);
            endpoint.setTcpHostAndPort(value.value);
            break;
        case P2P_ENDPOINT:
            ClusterMetaData.updatePeerInfo(endpoint, "p2p_endpoint", value.value);
            break;
        case DC:
            ClusterMetaData.updatePeerInfo(endpoint, "data_center", value.value);
            break;
        case RACK:
            ClusterMetaData.updatePeerInfo(endpoint, "rack", value.value);
            break;
        case RELEASE_VERSION:
            ClusterMetaData.updatePeerInfo(endpoint, "release_version", value.value);
            break;
        case NET_VERSION:
            ClusterMetaData.updatePeerInfo(endpoint, "net_version", value.value);
            break;
        }
    }

    private void updatePeerInfo(NetEndpoint endpoint) {
        if (endpoint.equals(ConfigDescriptor.getLocalEndpoint()))
            return;

        EndpointState epState = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            updatePeerInfo(endpoint, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param endpoint node
     */
    private void handleStateNormal(final NetEndpoint endpoint) {
        Set<NetEndpoint> endpointsToRemove = new HashSet<>();

        if (topologyMetaData.isMember(endpoint))
            logger.info("Node {} state jump to normal", endpoint);

        updatePeerInfo(endpoint);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see Cassandra-4300).
        if (Gossiper.instance.usesHostId(endpoint)) {
            String hostId = Gossiper.instance.getHostId(endpoint);
            NetEndpoint existing = topologyMetaData.getEndpoint(hostId);

            if (existing != null && !existing.equals(endpoint)) {
                if (existing.equals(ConfigDescriptor.getLocalEndpoint())) {
                    logger.warn("Not updating host ID {} for {} because it's mine", hostId, endpoint);
                    topologyMetaData.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                } else if (Gossiper.instance.compareEndpointStartup(endpoint, existing) > 0) {
                    logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing,
                            endpoint, endpoint);
                    topologyMetaData.removeEndpoint(existing);
                    endpointsToRemove.add(existing);
                    topologyMetaData.updateHostId(hostId, endpoint);
                } else {
                    logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, endpoint,
                            endpoint);
                    topologyMetaData.removeEndpoint(endpoint);
                    endpointsToRemove.add(endpoint);
                }
            } else
                topologyMetaData.updateHostId(hostId, endpoint);
        }

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onJoinCluster(endpoint);
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param endpoint node
     */
    private void handleStateLeaving(NetEndpoint endpoint) {
        if (!topologyMetaData.isMember(endpoint)) {
            logger.info("Node {} state jump to leaving", endpoint);
        }
        // at this point the endpoint is certainly a member with this token, so let's proceed
        // normally
        topologyMetaData.addLeavingEndpoint(endpoint);
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param endpoint If reason for leaving is decommission, endpoint is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(NetEndpoint endpoint, String[] pieces) {
        excise(endpoint, extractExpireTime(pieces));
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param endpoint node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(NetEndpoint endpoint, String[] pieces) {
        assert (pieces.length > 0);

        if (endpoint.equals(ConfigDescriptor.getLocalEndpoint())) {
            logger.info(
                    "Received removenode gossip about myself. Is this node rejoining after an explicit removenode?");
            try {
                // drain();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }
        if (topologyMetaData.isMember(endpoint)) {
            String state = pieces[0];
            if (VersionedValue.REMOVED_TOKEN.equals(state)) {
                excise(endpoint, extractExpireTime(pieces));
            } else if (VersionedValue.REMOVING_TOKEN.equals(state)) {

                // Note that the endpoint is being removed
                topologyMetaData.addLeavingEndpoint(endpoint);
            }
        } else {
            // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(endpoint, extractExpireTime(pieces));
            removeEndpoint(endpoint);
        }
    }

    private void excise(NetEndpoint endpoint) {
        logger.info("Removing endpoint {} ", endpoint);
        removeEndpoint(endpoint);
        topologyMetaData.removeEndpoint(endpoint);

        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(endpoint);
    }

    private void excise(NetEndpoint endpoint, long expireTime) {
        addExpireTimeIfFound(endpoint, expireTime);
        excise(endpoint);
    }

    /** unlike excise we just need this endpoint gone without going through any notifications **/
    private void removeEndpoint(NetEndpoint endpoint) {
        Gossiper.instance.removeEndpoint(endpoint);
        ClusterMetaData.removeEndpoint(endpoint);
    }

    private void addExpireTimeIfFound(NetEndpoint endpoint, long expireTime) {
        if (expireTime != 0L) {
            Gossiper.instance.addExpireTimeForEndpoint(endpoint, expireTime);
        }
    }

    private long extractExpireTime(String[] pieces) {
        return Long.parseLong(pieces[2]);
    }

    @Override
    public void onJoin(NetEndpoint endpoint, EndpointState epState) {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            onChange(endpoint, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void onAlive(NetEndpoint endpoint, EndpointState state) {
        if (topologyMetaData.isMember(endpoint)) {
            for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onUp(endpoint);
        }
    }

    @Override
    public void onRemove(NetEndpoint endpoint) {
        topologyMetaData.removeEndpoint(endpoint);
    }

    @Override
    public void onDead(NetEndpoint endpoint, EndpointState state) {
        MessagingService.instance().convict(endpoint);
        for (IEndpointLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onDown(endpoint);
    }

    @Override
    public void onRestart(NetEndpoint endpoint, EndpointState state) {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(endpoint, state);
    }

    /** raw load value */
    public double getLoad() {
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        return os.getSystemLoadAverage();
    }

    public String getLoadString() {
        return FileUtils.stringifyFileSize(getLoad());
    }

    public Map<String, String> getLoadMap() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<NetEndpoint, Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet()) {
            map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(ConfigDescriptor.getLocalEndpoint().getHostAddress(), getLoadString());
        return map;
    }

    public String getReleaseVersion() {
        return Utils.getReleaseVersionString();
    }

    public List<String> getLeavingNodes() {
        return stringify(topologyMetaData.getLeavingEndpoints());
    }

    public List<String> getLiveNodes() {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes() {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    private List<String> stringify(Iterable<NetEndpoint> endpoints) {
        List<String> stringEndpoints = new ArrayList<>();
        for (NetEndpoint ep : endpoints) {
            stringEndpoints.add(ep.getHostAddress());
        }
        return stringEndpoints;
    }

    /**
     * This method returns the N endpoints that are responsible for storing the
     * specified key i.e for replication.
     *
     * @param db the database
     * @param pos position for which we need to find the endpoint
     * @return the endpoint responsible for this token
     */
    public List<NetEndpoint> getReplicationEndpoints(Database db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints) {
        return getReplicationEndpoints(db, oldReplicationEndpoints, candidateEndpoints, false);
    }

    public List<NetEndpoint> getReplicationEndpoints(Database db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints, boolean includeOldReplicationEndpoints) {
        return ClusterMetaData.getReplicationStrategy(db).getReplicationEndpoints(oldReplicationEndpoints,
                candidateEndpoints, includeOldReplicationEndpoints);
    }

    public List<NetEndpoint> getLiveReplicationEndpoints(Database db, Set<NetEndpoint> oldReplicationEndpoints,
            Set<NetEndpoint> candidateEndpoints, boolean includeOldReplicationEndpoints) {
        List<NetEndpoint> endpoints = getReplicationEndpoints(db, oldReplicationEndpoints, candidateEndpoints,
                includeOldReplicationEndpoints);
        List<NetEndpoint> liveEps = new ArrayList<>(endpoints.size());
        for (NetEndpoint endpoint : endpoints) {
            if (FailureDetector.instance.isAlive(endpoint))
                liveEps.add(endpoint);
        }
        return liveEps;
    }

    /**
     * Remove a node that has died, attempting to restore the replica count.
     * If the node is alive, decommission should be attempted.  If decommission
     * fails, then removeToken should be called.  If we fail while trying to
     * restore the replica count, finally forceRemoveCompleteion should be
     * called to forcibly remove the node without regard to replica count.
     *
     * @param hostIdString token for the node
     */
    public void removeNode(String hostIdString) {
    }

    public void decommission() {
    }

    @Override
    public boolean isRunning(boolean traceError) {
        return started;
    }

    @Override
    public String getURL() {
        return "p2p://" + getHost() + ":" + getPort();
    }

    @Override
    public int getPort() {
        return port;
    }

    public boolean isSSL() {
        return ssl;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public String getName() {
        return getType();
    }

    @Override
    public String getType() {
        return this.getClass().getSimpleName();
    }

    @Override
    public boolean getAllowOthers() {
        return true;
    }

    @Override
    public boolean isDaemon() {
        return true;
    }

    @Override
    public void setServerEncryptionOptions(ServerEncryptionOptions options) {
        this.options = options;
    }

    public ServerEncryptionOptions getServerEncryptionOptions() {
        return options;
    }
}
