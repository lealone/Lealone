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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.async.AsyncPeriodicTask;
import org.lealone.db.async.AsyncTaskHandlerFactory;
import org.lealone.net.NetEndpoint;
import org.lealone.p2p.gms.ApplicationState;
import org.lealone.p2p.gms.EndpointState;
import org.lealone.p2p.gms.Gossiper;
import org.lealone.p2p.gms.IEndpointStateChangeSubscriber;
import org.lealone.p2p.gms.VersionedValue;

public class LoadBroadcaster implements IEndpointStateChangeSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(LoadBroadcaster.class);
    private static final ConcurrentMap<NetEndpoint, Double> loadInfo = new ConcurrentHashMap<>();
    private static final int BROADCAST_INTERVAL = 60 * 1000;

    public static final LoadBroadcaster instance = new LoadBroadcaster();

    private LoadBroadcaster() {
        Gossiper.instance.register(this);
    }

    public void startBroadcasting() {
        // send the first broadcast "right away"
        // (i.e., in 2 gossip heartbeats, when we should have someone to talk to);
        // after that send every BROADCAST_INTERVAL.
        AsyncPeriodicTask task = new AsyncPeriodicTask() {
            @Override
            public void run() {
                if (logger.isDebugEnabled())
                    logger.debug("Disseminating load info ...");
                Gossiper.instance.addLocalApplicationState(ApplicationState.LOAD,
                        P2pServer.valueFactory.load(P2pServer.instance.getLoad()));
            }
        };
        AsyncTaskHandlerFactory.getAsyncTaskHandler().scheduleWithFixedDelay(task, 2 * Gossiper.INTERVAL_IN_MILLIS,
                BROADCAST_INTERVAL, TimeUnit.MILLISECONDS);
    }

    public Map<NetEndpoint, Double> getLoadInfo() {
        return Collections.unmodifiableMap(loadInfo);
    }

    @Override
    public void onChange(NetEndpoint endpoint, ApplicationState state, VersionedValue value) {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo.put(endpoint, Double.valueOf(value.value));
    }

    @Override
    public void onJoin(NetEndpoint endpoint, EndpointState epState) {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null) {
            onChange(endpoint, ApplicationState.LOAD, localValue);
        }
    }

    @Override
    public void onRemove(NetEndpoint endpoint) {
        loadInfo.remove(endpoint);
    }
}
