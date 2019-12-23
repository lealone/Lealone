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
package org.lealone.p2p.gossip.protocol;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.lealone.net.NetNode;
import org.lealone.net.TransferOutputStream;
import org.lealone.p2p.config.ConfigDescriptor;

public class P2pPacketOut<T extends P2pPacket> {

    public final NetNode from;
    public final T packet;
    public final Map<String, byte[]> parameters;

    public P2pPacketOut(T payload) {
        this(payload, Collections.emptyMap());
    }

    private P2pPacketOut(T packet, Map<String, byte[]> parameters) {
        this.from = ConfigDescriptor.getLocalNode();
        this.packet = packet;
        this.parameters = parameters;
    }

    public P2pPacketOut<T> withParameter(String key, byte[] value) {
        HashMap<String, byte[]> map = new HashMap<>(parameters);
        map.put(key, value);
        return new P2pPacketOut<>(packet, map);
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

    public void serialize(TransferOutputStream transferOut, DataOutput out, int version) throws IOException {
        from.serialize(out);

        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        // out.writeInt(packet.getType().value);
        packet.encode(transferOut, version);

        // out.writeInt(0); // 先设为0
        // if (packet != null) {
        // int lastSize = transferOut.getDataOutputStreamSize();
        // packet.encode(transferOut, version);
        //
        // int payloadSize = transferOut.getDataOutputStreamSize() - lastSize;
        // int payloadSizeStartPos = lastSize - 4;// 需要减掉4
        // transferOut.setPayloadSize(payloadSizeStartPos, payloadSize); // 再回填
        // }
    }
}
