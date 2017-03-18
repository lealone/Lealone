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
package org.lealone.aose.net;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.lealone.aose.concurrent.Stage;
import org.lealone.aose.config.ConfigDescriptor;
import org.lealone.aose.util.FileUtils;
import org.lealone.net.NetEndpoint;

import com.google.common.collect.ImmutableMap;

public class MessageIn<T> {
    public final NetEndpoint from;
    public final T payload;
    public final Map<String, byte[]> parameters;
    public final MessagingService.Verb verb;
    public final int version;

    private MessageIn(NetEndpoint from, T payload, Map<String, byte[]> parameters, MessagingService.Verb verb,
            int version) {
        this.from = from;
        this.payload = payload;
        this.parameters = parameters;
        this.verb = verb;
        this.version = version;
    }

    public Stage getMessageType() {
        return MessagingService.verbStages.get(verb);
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
        NetEndpoint from = CompactEndpointSerializationHelper.deserialize(in);

        MessagingService.Verb verb = MessagingService.Verb.values()[in.readInt()];
        int parameterCount = in.readInt();
        Map<String, byte[]> parameters;
        if (parameterCount == 0) {
            parameters = Collections.emptyMap();
        } else {
            ImmutableMap.Builder<String, byte[]> builder = ImmutableMap.builder();
            for (int i = 0; i < parameterCount; i++) {
                String key = in.readUTF();
                byte[] value = new byte[in.readInt()];
                in.readFully(value);
                builder.put(key, value);
            }
            parameters = builder.build();
        }

        int payloadSize = in.readInt();
        IVersionedSerializer<?> serializer = MessagingService.verbSerializers.get(verb);
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
            return new MessageIn<>(from, null, parameters, verb, version);

        Object payload = serializer.deserialize(in, version);
        return new MessageIn<>(from, payload, parameters, verb, version);
    }
}
