/**
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
package org.lealone.aose.locator;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.gms.ApplicationState;
import org.lealone.aose.gms.EndpointState;
import org.lealone.aose.gms.Gossiper;
import org.lealone.aose.server.ClusterMetaData;
import org.lealone.aose.server.P2pServer;
import org.lealone.aose.util.ResourceWatcher;
import org.lealone.aose.util.Utils;
import org.lealone.aose.util.WrappedRunnable;
import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.net.NetEndpoint;

public class GossipingPropertyFileSnitch extends AbstractNetworkTopologySnitch {
    private static final Logger logger = LoggerFactory.getLogger(GossipingPropertyFileSnitch.class);

    private volatile String myDC;
    private volatile String myRack;
    private volatile boolean preferLocal;
    private final AtomicReference<ReconnectableSnitchHelper> snitchHelperReference;
    private volatile boolean gossipStarted;

    private Map<NetEndpoint, Map<String, String>> savedEndpoints;
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
            Utils.resourceToFile(fileName);
            Runnable runnable = new WrappedRunnable() {
                @Override
                protected void runMayThrow() throws ConfigException {
                    reloadConfiguration();
                }
            };
            ResourceWatcher.watch(fileName, runnable, refreshPeriodInSeconds * 1000);
        } catch (ConfigException ex) {
            logger.error("{} found, but does not look like a plain file. Will not watch it for changes", fileName);
        }
    }

    /**
     * Return the data center for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of data center
     */
    @Override
    public String getDatacenter(NetEndpoint endpoint) {
        if (endpoint.equals(ConfigDescriptor.getLocalEndpoint()))
            return myDC;

        EndpointState epState = Gossiper.instance.getEndpointState(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.DC) == null) {
            if (savedEndpoints == null)
                savedEndpoints = ClusterMetaData.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("data_center");
            return DEFAULT_DC;
        }
        return epState.getApplicationState(ApplicationState.DC).value;
    }

    /**
     * Return the rack for which an endpoint resides in
     *
     * @param endpoint the endpoint to process
     * @return string of rack
     */
    @Override
    public String getRack(NetEndpoint endpoint) {
        if (endpoint.equals(ConfigDescriptor.getLocalEndpoint()))
            return myRack;

        EndpointState epState = Gossiper.instance.getEndpointState(endpoint);
        if (epState == null || epState.getApplicationState(ApplicationState.RACK) == null) {
            if (savedEndpoints == null)
                savedEndpoints = ClusterMetaData.loadDcRackInfo();
            if (savedEndpoints.containsKey(endpoint))
                return savedEndpoints.get(endpoint).get("rack");
            return DEFAULT_RACK;
        }
        return epState.getApplicationState(ApplicationState.RACK).value;
    }

    @Override
    public void gossiperStarting() {
        super.gossiperStarting();

        Gossiper.instance.addLocalApplicationState(ApplicationState.INTERNAL_IP,
                P2pServer.valueFactory.internalIP(ConfigDescriptor.getLocalEndpoint().getHostAddress()));

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
