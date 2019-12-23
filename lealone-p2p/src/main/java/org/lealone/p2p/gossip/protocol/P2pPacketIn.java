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
package org.lealone.p2p.gossip.protocol;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.lealone.net.NetNode;
import org.lealone.net.TransferInputStream;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.server.MessagingService;
import org.lealone.server.protocol.Packet;
import org.lealone.server.protocol.PacketDecoder;
import org.lealone.server.protocol.PacketDecoders;

public class P2pPacketIn<T extends P2pPacket> {

    public final NetNode from;
    public final T packet;
    public final Map<String, byte[]> parameters;
    public final int version;

    private P2pPacketIn(NetNode from, T packet, Map<String, byte[]> parameters, int version) {
        this.from = from;
        this.packet = packet;
        this.parameters = parameters;
        this.version = version;
    }

    public boolean doCallbackOnFailure() {
        return parameters.containsKey(MessagingService.FAILURE_CALLBACK_PARAM);
    }

    public boolean isFailureResponse() {
        return parameters.containsKey(MessagingService.FAILURE_RESPONSE_PARAM);
    }

    public long getTimeout() {
        return ConfigDescriptor.getTimeout(packet.getType());
    }

    @Override
    public String toString() {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("FROM: ").append(from).append(" Packet: ").append(packet.getType());
        return sbuf.toString();
    }

    public static P2pPacketIn<?> read(TransferInputStream transfer, DataInput in, int version, int id, int packetType)
            throws IOException {
        NetNode from = NetNode.deserialize(in);
        int parameterCount = in.readInt();
        Map<String, byte[]> parameters;
        if (parameterCount == 0) {
            parameters = Collections.emptyMap();
        } else {
            parameters = new HashMap<>(parameterCount);
            for (int i = 0; i < parameterCount; i++) {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                parameters.put(key, value);
            }
        }
        // int payloadSize = in.readInt();
        // CallbackInfo callback = MessagingService.instance().getRegisteredCallback(id);
        // if (callback == null) {
        // // reply for expired callback. we'll have to skip it.
        // FileUtils.skipBytesFully(in, payloadSize);
        // return null;
        // }
        // if (payloadSize == 0)
        // return new P2pPacketIn<>(from, null, parameters, version);

        PacketDecoder<? extends Packet> decoder = PacketDecoders.getDecoder(packetType);
        P2pPacket packet = (P2pPacket) decoder.decode(transfer, version);
        return new P2pPacketIn<>(from, packet, parameters, version);
    }
}
