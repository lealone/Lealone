/**
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetNode;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.gossip.ApplicationState;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.gossip.NodeState;
import org.lealone.p2p.server.ClusterMetaData;
import org.lealone.p2p.server.P2pServer;
import org.lealone.p2p.util.ResourceWatcher;
import org.lealone.p2p.util.P2pUtils;

public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch {
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    private volatile String myDC;
    private volatile String myRack;
    private volatile boolean preferLocal;
    private final AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;
    private volatile boolean gossipStarted;

    private Map<NetNode, Map<String, String>> savedNodes;
    private static final String DEFAULT_DC = "UNKNOWN_DC";
    private static final String DEFAULT_RACK = "UNKNOWN_RACK";

    private static final int DEFAULT_REFRESH_PERIOD_IN_SECONDS = 60;

    public GossipingPropertyFileSnitch() throws ConfigException {
        this(DEFAULT_REFRESH_PERIOD_IN_SECONDS);
    }

    public GossipingPropertyFileSnitch(int refreshPeriodInSeconds) throws ConfigException {
        snitchHelperReference = new AtomicReference<ReconnectableSnitchHelper>();

        reloadConfiguration();

        String fileName = null;
        try {
            fileName = System.getProperty(SnitchProperties.RACKDC_PROPERTY_FILENAME);
            if (fileName == null)
                fileName = SnitchProperties.RACKDC_PROPERTY_FILENAME;
            P2pUtils.resourceToFile(fileName);
            Runnable runnable = () -> {
                try {
                    reloadConfiguration();
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            };
            ResourceWatcher.watch(fileName, runnable, refreshPeriodInSeconds * 1000);
        } catch (ConfigException ex) {
            logger.error("{} found, but does not look like a plain file. Will not watch it for changes", fileName);
        }
    }

    /**
     * Return the data center for which an node resides in
     *
     * @param node the node to process
     * @return string of data center
     */
    @Override
    public String getDatacenter(NetNode node) {
        if (node.equals(ConfigDescriptor.getLocalNode()))
            return myDC;

        NodeState epState = Gossiper.instance.getNodeState(node);
        if (epState == null || epState.getApplicationState(ApplicationState.DC) == null) {
            if (savedNodes == null)
                savedNodes = ClusterMetaData.loadDcRackInfo();
            if (savedNodes.containsKey(node))
                return savedNodes.get(node).get("data_center");
            return DEFAULT_DC;
        }
        return epState.getApplicationState(ApplicationState.DC).value;
    }

    /**
     * Return the rack for which an node resides in
     *
     * @param node the node to process
     * @return string of rack
     */
    @Override
    public String getRack(NetNode node) {
        if (node.equals(ConfigDescriptor.getLocalNode()))
            return myRack;

        NodeState epState = Gossiper.instance.getNodeState(node);
        if (epState == null || epState.getApplicationState(ApplicationState.RACK) == null) {
            if (savedNodes == null)
                savedNodes = ClusterMetaData.loadDcRackInfo();
            if (savedNodes.containsKey(node))
                return savedNodes.get(node).get("rack");
            return DEFAULT_RACK;
        }
        return epState.getApplicationState(ApplicationState.RACK).value;
    }

    @Override
    public void gossiperStarting() {
        super.gossiperStarting();

        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                P2pServer.valueFactory.internalIP(ConfigDescriptor.getLocalNode().getHostAddress()));

        reloadGossiperState();

        gossipStarted = true;
    }

    private void reloadConfiguration() throws ConfigException {
        final SnitchProperties properties = new SnitchProperties();

        String newDc = properties.get("dc", null);
        String newRack = properties.get("rack", null);
        if (newDc == null || newRack == null)
            throw new ConfigException("DC or rack not found in snitch properties, check your configuration in: "
                    + SnitchProperties.RACKDC_PROPERTY_FILENAME);

        newDc = newDc.trim();
        newRack = newRack.trim();
        final boolean newPreferLocal = Boolean.parseBoolean(properties.get("prefer_local", "false"));

        if (!newDc.equals(myDC) || !newRack.equals(myRack) || (preferLocal != newPreferLocal)) {
            myDC = newDc;
            myRack = newRack;
            preferLocal = newPreferLocal;

            reloadGossiperState();

            if (P2pServer.instance != null)
                P2pServer.instance.getTopologyMetaData().invalidateCachedRings();

            if (gossipStarted)
                P2pServer.instance.gossipSnitchInfo();
        }
    }

    private void reloadGossiperState() {
        if (Gossiper.instance != null) {
            ReconnectableSnitchHelper pendingHelper = new ReconnectableSnitchHelper(this, myDC, preferLocal);
            Gossiper.instance.register(pendingHelper);

            pendingHelper = snitchHelperReference.getAndSet(pendingHelper);
            if (pendingHelper != null)
                Gossiper.instance.unregister(pendingHelper);
        }
        // else this will eventually rerun at gossiperStarting()
    }
}
