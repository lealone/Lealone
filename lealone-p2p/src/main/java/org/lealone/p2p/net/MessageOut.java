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
package org.lealone.p2p.net;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.lealone.net.NetNode;
import org.lealone.net.TransferOutputStream;
import org.lealone.p2p.config.ConfigDescriptor;

public class MessageOut<T extends Message<T>> {
    public final NetNode from;
    public final Verb verb;
    public final T payload;
    public final Map<String, byte[]> parameters;

    // we do support messages that just consist of a verb
    public MessageOut(Verb verb) {
        this(verb, null);
    }

    public MessageOut(Verb verb, T payload) {
        this(verb, payload, Collections.emptyMap());
    }

    private MessageOut(Verb verb, T payload, Map<String, byte[]> parameters) {
        this.from = ConfigDescriptor.getLocalNode();
        this.verb = verb;
        this.payload = payload;
        this.parameters = parameters;
    }

    public MessageOut<T> withParameter(String key, byte[] value) {
        HashMap<String, byte[]> map = new HashMap<>(parameters);
        map.put(key, value);
        return new MessageOut<>(verb, payload, map);
    }

    public long getTimeout() {
        return ConfigDescriptor.getTimeout(verb);
    }

    @Override
    public String toString() {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(" VERB:").append(verb);
        return sbuf.toString();
    }

    public void serialize(TransferOutputStream transferOut, DataOutput out, int version) throws IOException {
        from.serialize(out);

        out.writeInt(verb.ordinal());
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        out.writeInt(0); // 先设为0
        if (payload != null) {
            int lastSize = transferOut.getDataOutputStreamSize();
            payload.getSerializer().serialize(payload, out, version);

            int payloadSize = transferOut.getDataOutputStreamSize() - lastSize;
            int payloadSizeStartPos = lastSize - 4;// 需要减掉4
            transferOut.setPayloadSize(payloadSizeStartPos, payloadSize); // 再回填
        }
    }
}
