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

import org.lealone.net.NetEndpoint;
import org.lealone.net.Transfer;
import org.lealone.p2p.concurrent.Stage;
import org.lealone.p2p.config.ConfigDescriptor;

public class MessageOut<T extends Message<T>> {
    public final NetEndpoint from;
    public final Verb verb;
    public final T payload;
    public final Map<String, byte[]> parameters;

    // we do support messages that just consist of a verb
    public MessageOut(Verb verb) {
        this(verb, null);
    }

    public MessageOut(Verb verb, T payload) {
        this(verb, payload, Collections.<String, byte[]> emptyMap());
    }

    private MessageOut(Verb verb, T payload, Map<String, byte[]> parameters) {
        this(ConfigDescriptor.getLocalEndpoint(), verb, payload, parameters);
    }

    public MessageOut(NetEndpoint from, Verb verb, T payload, Map<String, byte[]> parameters) {
        this.from = from;
        this.verb = verb;
        this.payload = payload;
        this.parameters = parameters;
    }

    public MessageOut<T> withParameter(String key, byte[] value) {
        HashMap<String, byte[]> map = new HashMap<>(parameters);
        map.put(key, value);
        return new MessageOut<T>(verb, payload, map);
    }

    public Stage getStage() {
        return verb.stage;
    }

    public long getTimeout() {
        return ConfigDescriptor.getTimeout(verb);
    }

    @Override
    public String toString() {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("TYPE:").append(getStage()).append(" VERB:").append(verb);
        return sbuf.toString();
    }

    public int serialize(Transfer transfer, DataOutput out, int version) throws IOException {
        CompactEndpointSerializationHelper.serialize(from, out);

        out.writeInt(verb.ordinal());
        out.writeInt(parameters.size());
        for (Map.Entry<String, byte[]> entry : parameters.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeInt(entry.getValue().length);
            out.write(entry.getValue());
        }

        // long longSize = payload == null ? 0 : serializer.serializedSize(payload, version);
        // assert longSize <= Integer.MAX_VALUE; // larger values are supported in sstables but not messages
        int payloadStartPos = transfer.getDataOutputStreamSize();
        // out.writeInt((int) longSize);
        out.writeInt(0); // 写设为0，会回填
        if (payload != null)
            payload.getSerializer().serialize(payload, out, version);
        return payloadStartPos;
    }
}
