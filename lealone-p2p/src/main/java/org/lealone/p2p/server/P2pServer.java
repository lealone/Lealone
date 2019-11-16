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
package org.lealone.p2p.server;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.Pair;
import org.lealone.db.Constants;
import org.lealone.net.AsyncConnection;
import org.lealone.net.AsyncConnectionManager;
import org.lealone.net.NetNode;
import org.lealone.net.NetFactory;
import org.lealone.net.NetFactoryManager;
import org.lealone.net.NetServer;
import org.lealone.net.WritableChannel;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gms.ApplicationState;
import org.lealone.p2p.gms.NodeState;
import org.lealone.p2p.gms.Gossiper;
import org.lealone.p2p.gms.INodeStateChangeSubscriber;
import org.lealone.p2p.gms.VersionedValue;
import org.lealone.p2p.gms.VersionedValue.VersionedValueFactory;
import org.lealone.p2p.locator.INodeSnitch;
import org.lealone.p2p.locator.TopologyMetaData;
import org.lealone.p2p.net.MessagingService;
import org.lealone.p2p.net.P2pConnection;
import org.lealone.p2p.util.FileUtils;
import org.lealone.p2p.util.Utils;
import org.lealone.server.DelegatedProtocolServer;

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
@SuppressWarnings("restriction")
public class P2pServer extends DelegatedProtocolServer implements INodeStateChangeSubscriber, AsyncConnectionManager {

    private static final Logger logger = LoggerFactory.getLogger(P2pServer.class);
    private static final BackgroundActivityMonitor bgMonitor = new BackgroundActivityMonitor();

    public static final P2pServer instance = new P2pServer();
    public static final VersionedValueFactory valueFactory = new VersionedValueFactory();

    private final TopologyMetaData topologyMetaData = new TopologyMetaData();
    private final List<INodeLifecycleSubscriber> lifecycleSubscribers = new CopyOnWriteArrayList<>();

    private boolean gossipingStarted;

    private String localHostId;

    // 单例
    private P2pServer() {
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public String getType() {
        return P2pServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        if (!config.containsKey("port"))
            config.put("port", String.valueOf(Constants.DEFAULT_P2P_PORT));

        NetFactory factory = NetFactoryManager.getFactory(config);
        NetServer netServer = factory.createNetServer();
        netServer.setConnectionManager(this);
        setProtocolServer(netServer);
        netServer.init(config);

        NetNode.setLocalP2pNode(getHost(), getPort());
    }

    @Override
    public synchronized void stop() {
        if (isStopped())
            return;
        super.stop();
        Gossiper.instance.stop();
        MessagingService.instance().shutdown();
    }

    @Override
    public synchronized void start() throws ConfigException {
        if (isStarted())
            return;

        loadPersistedNodeInfo();
        prepareToJoin();
        joinCluster();
        super.start();
    }

    private void loadPersistedNodeInfo() {
        if (Boolean.parseBoolean(Config.getProperty("load.persisted.node.info", "true"))) {
            logger.info("Loading persisted node info");
            Map<NetNode, String> loadedHostIds = ClusterMetaData.loadHostIds();
            NetNode local = ConfigDescriptor.getLocalNode();
            for (NetNode ep : loadedHostIds.keySet()) {
                if (!ep.equals(local)) {
                    topologyMetaData.updateHostId(loadedHostIds.get(ep), ep);
                    Gossiper.instance.addSavedNode(ep);
                }
            }
        }
    }

    private void prepareToJoin() throws ConfigException {
        NetNode localNode = ConfigDescriptor.getLocalNode();
        localHostId = NetNode.getLocalTcpHostAndPort();
        topologyMetaData.updateHostId(localHostId, localNode);

        int generation = ClusterMetaData.incrementAndGetGeneration(localNode);

        Map<ApplicationState, VersionedValue> appStates = new HashMap<>();
        appStates.put(ApplicationState.HOST_ID, valueFactory.hostId(localHostId));
        appStates.put(ApplicationState.TCP_NODE, valueFactory.node(NetNode.getLocalTcpNode()));
        appStates.put(ApplicationState.P2P_NODE, valueFactory.node(NetNode.getLocalP2pNode()));
        appStates.put(ApplicationState.DC, getDatacenter());
        appStates.put(ApplicationState.RACK, getRack());
        appStates.put(ApplicationState.RELEASE_VERSION, valueFactory.releaseVersion());
        appStates.put(ApplicationState.NET_VERSION, valueFactory.networkVersion());

        // 先启动MessagingService再启动Gossiper
        MessagingService.instance().start();

        logger.info("Starting up server gossip");
        Gossiper.instance.register(this);
        Gossiper.instance.start(generation, appStates);

        logger.info("Starting up LoadBroadcaster");
        LoadBroadcaster.instance.startBroadcasting();
    }

    private VersionedValue getDatacenter() {
        INodeSnitch snitch = ConfigDescriptor.getNodeSnitch();
        String dc = snitch.getDatacenter(ConfigDescriptor.getLocalNode());
        return valueFactory.datacenter(dc);
    }

    private VersionedValue getRack() {
        INodeSnitch snitch = ConfigDescriptor.getNodeSnitch();
        String rack = snitch.getRack(ConfigDescriptor.getLocalNode());
        return valueFactory.rack(rack);
    }

    private void joinCluster() {
        List<Pair<ApplicationState, VersionedValue>> states = new ArrayList<>(1);
        states.add(Pair.create(ApplicationState.STATUS, valueFactory.normal(localHostId)));
        Gossiper.instance.addLocalApplicationStates(states);
    }

    // gossip snitch infos (local DC and rack)
    public void gossipSnitchInfo() {
        Gossiper.instance.addLocalApplicationState(ApplicationState.DC, getDatacenter());
        Gossiper.instance.addLocalApplicationState(ApplicationState.RACK, getRack());
    }

    public void register(INodeLifecycleSubscriber subscriber) {
        lifecycleSubscribers.add(subscriber);
    }

    public void unregister(INodeLifecycleSubscriber subscriber) {
        lifecycleSubscribers.remove(subscriber);
    }

    public void startGossiping() {
        if (!gossipingStarted) {
            logger.warn("Starting gossip by operator request");
            Gossiper.instance.start((int) (System.currentTimeMillis() / 1000));
            gossipingStarted = true;
        }
    }

    public void stopGossiping() {
        if (gossipingStarted) {
            logger.warn("Stopping gossip by operator request");
            Gossiper.instance.stop();
            gossipingStarted = false;
        }
    }

    public boolean isGossipRunning() {
        return Gossiper.instance.isEnabled();
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

    public double getSeverity(NetNode node) {
        return bgMonitor.getSeverity(node);
    }

    public String getLocalHostId() {
        return localHostId;
    }

    public String getLocalHostIdAsString() {
        return topologyMetaData.getHostId(ConfigDescriptor.getLocalNode());
    }

    public Map<String, String> getHostIdMap() {
        Map<String, String> mapOut = new HashMap<>();
        for (Map.Entry<NetNode, String> entry : topologyMetaData.getNodeToHostIdMapForReading().entrySet())
            mapOut.put(entry.getKey().getHostAddress(), entry.getValue().toString());
        return mapOut;
    }

    @Override
    public void onChange(NetNode node, ApplicationState state, VersionedValue value) {
        if (state == ApplicationState.STATUS) {
            String apStateValue = value.value;
            String[] pieces = apStateValue.split(VersionedValue.DELIMITER_STR, -1);
            assert (pieces.length > 0);

            String moveName = pieces[0];

            switch (moveName) {
            case VersionedValue.STATUS_NORMAL:
                handleStateNormal(node);
                break;
            case VersionedValue.REMOVING_TOKEN:
            case VersionedValue.REMOVED_TOKEN:
                handleStateRemoving(node, pieces);
                break;
            case VersionedValue.STATUS_LEAVING:
                handleStateLeaving(node);
                break;
            case VersionedValue.STATUS_LEFT:
                handleStateLeft(node, pieces);
                break;
            }
        } else {
            NodeState epState = Gossiper.instance.getNodeState(node);
            if (epState == null || Gossiper.instance.isDeadState(epState)) {
                logger.debug("Ignoring state change for dead or unknown node: {}", node);
                return;
            }
            if (!node.equals(ConfigDescriptor.getLocalNode()))
                updatePeerInfo(node, state, value);
        }
    }

    private void updatePeerInfo(NetNode node, ApplicationState state, VersionedValue value) {
        switch (state) {
        case HOST_ID:
            ClusterMetaData.updatePeerInfo(node, "host_id", value.value);
            break;
        case TCP_NODE:
            ClusterMetaData.updatePeerInfo(node, "tcp_node", value.value);
            node.setTcpHostAndPort(value.value);
            break;
        case P2P_NODE:
            ClusterMetaData.updatePeerInfo(node, "p2p_node", value.value);
            break;
        case DC:
            ClusterMetaData.updatePeerInfo(node, "data_center", value.value);
            break;
        case RACK:
            ClusterMetaData.updatePeerInfo(node, "rack", value.value);
            break;
        case RELEASE_VERSION:
            ClusterMetaData.updatePeerInfo(node, "release_version", value.value);
            break;
        case NET_VERSION:
            ClusterMetaData.updatePeerInfo(node, "net_version", value.value);
            break;
        }
    }

    private void updatePeerInfo(NetNode node) {
        if (node.equals(ConfigDescriptor.getLocalNode()))
            return;

        NodeState epState = Gossiper.instance.getNodeState(node);
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            updatePeerInfo(node, entry.getKey(), entry.getValue());
        }
    }

    /**
     * Handle node move to normal state. That is, node is entering token ring and participating
     * in reads.
     *
     * @param node node
     */
    private void handleStateNormal(final NetNode node) {
        Set<NetNode> nodesToRemove = new HashSet<>();

        if (topologyMetaData.isMember(node))
            logger.info("Node {} state jump to normal", node);

        updatePeerInfo(node);
        // Order Matters, TM.updateHostID() should be called before TM.updateNormalToken(), (see Cassandra-4300).
        if (Gossiper.instance.usesHostId(node)) {
            String hostId = Gossiper.instance.getHostId(node);
            NetNode existing = topologyMetaData.getNode(hostId);

            if (existing != null && !existing.equals(node)) {
                if (existing.equals(ConfigDescriptor.getLocalNode())) {
                    logger.warn("Not updating host ID {} for {} because it's mine", hostId, node);
                    topologyMetaData.removeNode(node);
                    nodesToRemove.add(node);
                } else if (Gossiper.instance.compareNodeStartup(node, existing) > 0) {
                    logger.warn("Host ID collision for {} between {} and {}; {} is the new owner", hostId, existing,
                            node, node);
                    topologyMetaData.removeNode(existing);
                    nodesToRemove.add(existing);
                    topologyMetaData.updateHostId(hostId, node);
                } else {
                    logger.warn("Host ID collision for {} between {} and {}; ignored {}", hostId, existing, node, node);
                    topologyMetaData.removeNode(node);
                    nodesToRemove.add(node);
                }
            } else {
                topologyMetaData.updateHostId(hostId, node);
            }
        }

        for (INodeLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onJoinCluster(node);
    }

    /**
     * Handle node preparing to leave the ring
     *
     * @param node node
     */
    private void handleStateLeaving(NetNode node) {
        if (!topologyMetaData.isMember(node)) {
            logger.info("Node {} state jump to leaving", node);
        }
        // at this point the node is certainly a member with this token, so let's proceed normally
        topologyMetaData.addLeavingNode(node);
    }

    /**
     * Handle node leaving the ring. This will happen when a node is decommissioned
     *
     * @param node If reason for leaving is decommission, node is the leaving node.
     * @param pieces STATE_LEFT,token
     */
    private void handleStateLeft(NetNode node, String[] pieces) {
        excise(node, extractExpireTime(pieces));
    }

    /**
     * Handle notification that a node being actively removed from the ring via 'removenode'
     *
     * @param node node
     * @param pieces either REMOVED_TOKEN (node is gone) or REMOVING_TOKEN (replicas need to be restored)
     */
    private void handleStateRemoving(NetNode node, String[] pieces) {
        assert (pieces.length > 0);

        if (node.equals(ConfigDescriptor.getLocalNode())) {
            logger.info("Received removenode gossip about myself. "
                    + "Is this node rejoining after an explicit removenode?");
            try {
                // drain();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }
        if (topologyMetaData.isMember(node)) {
            String state = pieces[0];
            if (VersionedValue.REMOVED_TOKEN.equals(state)) {
                excise(node, extractExpireTime(pieces));
            } else if (VersionedValue.REMOVING_TOKEN.equals(state)) {

                // Note that the node is being removed
                topologyMetaData.addLeavingNode(node);
            }
        } else {
            // now that the gossiper has told us about this nonexistent member, notify the gossiper to remove it
            if (VersionedValue.REMOVED_TOKEN.equals(pieces[0]))
                addExpireTimeIfFound(node, extractExpireTime(pieces));
            removeNode(node);
        }
    }

    private void excise(NetNode node) {
        logger.info("Removing node {} ", node);
        removeNode(node);
        topologyMetaData.removeNode(node);

        for (INodeLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onLeaveCluster(node);
    }

    private void excise(NetNode node, long expireTime) {
        addExpireTimeIfFound(node, expireTime);
        excise(node);
    }

    /** unlike excise we just need this node gone without going through any notifications **/
    private void removeNode(NetNode node) {
        Gossiper.instance.removeNode(node);
        ClusterMetaData.removeNode(node);
    }

    private void addExpireTimeIfFound(NetNode node, long expireTime) {
        if (expireTime != 0L) {
            Gossiper.instance.addExpireTimeForNode(node, expireTime);
        }
    }

    private long extractExpireTime(String[] pieces) {
        return Long.parseLong(pieces[2]);
    }

    @Override
    public void onJoin(NetNode node, NodeState epState) {
        for (Map.Entry<ApplicationState, VersionedValue> entry : epState.getApplicationStateMap().entrySet()) {
            onChange(node, entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void onAlive(NetNode node, NodeState state) {
        if (topologyMetaData.isMember(node)) {
            for (INodeLifecycleSubscriber subscriber : lifecycleSubscribers)
                subscriber.onUp(node);
        }
    }

    @Override
    public void onRemove(NetNode node) {
        topologyMetaData.removeNode(node);
    }

    @Override
    public void onDead(NetNode node, NodeState state) {
        MessagingService.instance().convict(node);
        for (INodeLifecycleSubscriber subscriber : lifecycleSubscribers)
            subscriber.onDown(node);
    }

    @Override
    public void onRestart(NetNode node, NodeState state) {
        // If we have restarted before the node was even marked down, we need to reset the connection pool
        if (state.isAlive())
            onDead(node, state);
    }

    /** raw load value */
    public double getLoad() {
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        double load = os.getSystemLoadAverage();
        if (load < 0) {
            MemoryUsage mu = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
            double max = mu.getMax() * 1.0D;
            double used = mu.getUsed() * 1.0D;
            load = used / max;
        }
        return load;
    }

    public String getLoadString() {
        return FileUtils.stringifyFileSize(getLoad());
    }

    public Map<String, String> getLoadMap() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<NetNode, Double> entry : LoadBroadcaster.instance.getLoadInfo().entrySet()) {
            map.put(entry.getKey().getHostAddress(), FileUtils.stringifyFileSize(entry.getValue()));
        }
        // gossiper doesn't see its own updates, so we need to special-case the local node
        map.put(ConfigDescriptor.getLocalNode().getHostAddress(), getLoadString());
        return map;
    }

    public String getReleaseVersion() {
        return Utils.getReleaseVersionString();
    }

    public List<String> getLeavingNodes() {
        return stringify(topologyMetaData.getLeavingNodes());
    }

    public List<String> getLiveNodes() {
        return stringify(Gossiper.instance.getLiveMembers());
    }

    public List<String> getUnreachableNodes() {
        return stringify(Gossiper.instance.getUnreachableMembers());
    }

    private List<String> stringify(Iterable<NetNode> nodes) {
        List<String> stringNodes = new ArrayList<>();
        for (NetNode ep : nodes) {
            stringNodes.add(ep.getHostAddress());
        }
        return stringNodes;
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

    public boolean authenticate(String host) {
        return true;
        // return ConfigDescriptor.getInternodeAuthenticator().authenticate(socket.remoteAddress(), 990);
    }

    @Override
    public AsyncConnection createConnection(WritableChannel writableChannel, boolean isServer) {
        return MessagingService.instance().createConnection(writableChannel, isServer);
    }

    @Override
    public void removeConnection(AsyncConnection conn) {
        MessagingService.instance().removeConnection((P2pConnection) conn);
    }
}
