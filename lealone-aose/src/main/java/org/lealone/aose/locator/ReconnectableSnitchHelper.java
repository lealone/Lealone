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
package org.lealone.aose.locator;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.lealone.aose.gms.ApplicationState;
import org.lealone.aose.gms.EndpointState;
import org.lealone.aose.gms.IEndpointStateChangeSubscriber;
import org.lealone.aose.gms.VersionedValue;
import org.lealone.aose.net.MessagingService;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

/**
 * Sidekick helper for snitches that want to reconnect from one IP addr for a node to another.
 * Typically, this is for situations like EC2 where a node will have a public address and a private address,
 * where we connect on the public, discover the private, and reconnect on the private.
 */
public class ReconnectableSnitchHelper implements IEndpointStateChangeSubscriber {
    private static final Logger logger = LoggerFactory.getLogger(ReconnectableSnitchHelper.class);
    private final IEndpointSnitch snitch;
    private final String localDc;
    private final boolean preferLocal;

    public ReconnectableSnitchHelper(IEndpointSnitch snitch, String localDc, boolean preferLocal) {
        this.snitch = snitch;
        this.localDc = localDc;
        this.preferLocal = preferLocal;
    }

    private void reconnect(InetAddress publicAddress, VersionedValue localAddressValue) {
        try {
            InetAddress localAddress = InetAddress.getByName(localAddressValue.value);

            if (snitch.getDatacenter(publicAddress).equals(localDc)
                    && MessagingService.instance().getVersion(publicAddress) == MessagingService.CURRENT_VERSION
                    && !MessagingService.instance().getConnectionEndpoint(publicAddress).equals(localAddress)) {

                MessagingService.instance().reconnect(publicAddress, localAddress);

                if (logger.isDebugEnabled())
                    logger.debug(String.format("Intiated reconnect to an Internal IP %s for the %s", localAddress,
                            publicAddress));
            }
        } catch (UnknownHostException e) {
            logger.error("Error in getting the IP address resolved: ", e);
        }
    }

    @Override
    public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) {
        if (preferLocal && state == ApplicationState.INTERNAL_IP)
            reconnect(endpoint, value);
    }

    @Override
    public void onJoin(InetAddress endpoint, EndpointState epState) {
        if (preferLocal && epState.getApplicationState(ApplicationState.INTERNAL_IP) != null)
            reconnect(endpoint, epState.getApplicationState(ApplicationState.INTERNAL_IP));
    }

    @Override
    public void onAlive(InetAddress endpoint, EndpointState state) {
        onJoin(endpoint, state);
    }

    @Override
    public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey,
            VersionedValue newValue) {
    }

    @Override
    public void onDead(InetAddress endpoint, EndpointState state) {
    }

    @Override
    public void onRemove(InetAddress endpoint) {
    }

    @Override
    public void onRestart(InetAddress endpoint, EndpointState state) {
    }
}
