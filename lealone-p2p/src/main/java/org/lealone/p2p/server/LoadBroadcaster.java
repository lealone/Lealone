/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
import org.lealone.net.NetNode;
import org.lealone.p2p.gossip.ApplicationState;
import org.lealone.p2p.gossip.Gossiper;
import org.lealone.p2p.gossip.INodeStateChangeSubscriber;
import org.lealone.p2p.gossip.NodeState;
import org.lealone.p2p.gossip.VersionedValue;

public class LoadBroadcaster implements INodeStateChangeSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(LoadBroadcaster.class);
    private static final ConcurrentMap<NetNode, Double> loadInfo = new ConcurrentHashMap<>();
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

    public Map<NetNode, Double> getLoadInfo() {
        return Collections.unmodifiableMap(loadInfo);
    }

    @Override
    public void onChange(NetNode node, ApplicationState state, VersionedValue value) {
        if (state != ApplicationState.LOAD)
            return;
        loadInfo.put(node, Double.valueOf(value.value));
    }

    @Override
    public void onJoin(NetNode node, NodeState epState) {
        VersionedValue localValue = epState.getApplicationState(ApplicationState.LOAD);
        if (localValue != null) {
            onChange(node, ApplicationState.LOAD, localValue);
        }
    }

    @Override
    public void onRemove(NetNode node) {
        loadInfo.remove(node);
    }
}
