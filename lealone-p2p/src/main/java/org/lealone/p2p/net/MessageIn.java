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
package org.lealone.p2p.net;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.lealone.net.NetEndpoint;
import org.lealone.p2p.concurrent.Stage;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.util.FileUtils;

public class MessageIn<T> {
    public final NetEndpoint from;
    public final Verb verb;
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final int version;

    private MessageIn(NetEndpoint from, Verb verb, T payload, Map<String, byte[]> parameters, int version) {
        this.from = from;
        this.verb = verb;
        this.payload = payload;
        this.parameters = parameters;
        this.version = version;
    }

    public Stage getMessageType() {
        return verb.stage;
    }

    public boolean doCallbackOnFailure() {
        return parameters.containsKey(MessagingService.FAILURE_CALLBACK_PARAM);
    }

    public boolean isFailureResponse() {
        return parameters.containsKey(MessagingService.FAILURE_RESPONSE_PARAM);
    }

    public long getTimeout() {
        return ConfigDescriptor.getTimeout(verb);
    }

    @Override
    public String toString() {
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("FROM:").append(from).append(" TYPE:").append(getMessageType()).append(" VERB:").append(verb);
        return sbuf.toString();
    }

    public static MessageIn<?> read(DataInput in, int version, int id) throws IOException {
        NetEndpoint from = NetEndpoint.deserialize(in);

        Verb verb = Verb.values()[in.readInt()];
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

        int payloadSize = in.readInt();
        IVersionedSerializer<?> serializer = verb.serializer;
        if (serializer instanceof MessagingService.CallbackDeterminedSerializer) {
            CallbackInfo callback = MessagingService.instance().getRegisteredCallback(id);
            if (callback == null) {
                // reply for expired callback. we'll have to skip it.
                FileUtils.skipBytesFully(in, payloadSize);
                return null;
            }
            serializer = callback.serializer;
        }
        if (payloadSize == 0 || serializer == null)
            return new MessageIn<>(from, verb, null, parameters, version);

        Object payload = serializer.deserialize(in, version);
        return new MessageIn<>(from, verb, payload, parameters, version);
    }
}
