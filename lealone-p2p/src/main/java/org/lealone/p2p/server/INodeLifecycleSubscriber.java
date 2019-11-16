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

import org.lealone.net.NetNode;

/**
 * Interface on which interested parties can be notified of high level node
 * state changes.
 *
 * Note that while INodeStateChangeSubscriber notify about gossip related
 * changes (INodeStateChangeSubscriber.onJoin() is called when a node join
 * gossip), this interface allows to be notified about higher level events.
 */
public interface INodeLifecycleSubscriber {
    /**
     * Called when a new node joins the cluster, i.e. either has just been
     * bootstrapped or "instajoins".
     *
     * @param node the newly added node.
     */
    public void onJoinCluster(NetNode node);

    /**
     * Called when a new node leave the cluster (decommission or removeToken).
     *
     * @param node the node that is leaving.
     */
    public void onLeaveCluster(NetNode node);

    /**
     * Called when a node is marked UP.
     *
     * @param node the node marked UP.
     */
    public void onUp(NetNode node);

    /**
     * Called when a node is marked DOWN.
     *
     * @param node the node marked DOWN.
     */
    public void onDown(NetNode node);

    /**
     * Called when a node has moved (to a new token).
     *
     * @param node the node that has moved.
     */
    public void onMove(NetNode node);
}
