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
package org.lealone.p2p.gossip.handler;

import org.lealone.p2p.gossip.protocol.P2pPacket;
import org.lealone.p2p.gossip.protocol.P2pPacketIn;

/**
 * P2pPacketHandler provides the method that all packet handlers need to implement.
 * The concrete implementation of this interface would provide the functionality
 * for a given packet.
 */
public interface P2pPacketHandler<T extends P2pPacket> {
    /**
     * This method delivers a packet to the implementing class (if the implementing
     * class was registered by a call to P2pPacketHandlers.register).
     * Note that the caller should not be holding any locks when calling this method
     * because the implementation may be synchronized.
     *
     * @param packetIn - incoming packet that needs handling.
     * @param id
     */
    public void handle(P2pPacketIn<T> packetIn, int id);
}
