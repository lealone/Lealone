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
package com.codefollower.lealone.atomicdb.transport.messages;

import org.jboss.netty.buffer.ChannelBuffer;

import com.codefollower.lealone.atomicdb.transport.CBUtil;
import com.codefollower.lealone.atomicdb.transport.Message;

import java.nio.ByteBuffer;

/**
 * SASL challenge sent from client to server
 */
public class AuthChallenge extends Message.Response
{
    public static final Message.Codec<AuthChallenge> codec = new Message.Codec<AuthChallenge>()
    {
        public AuthChallenge decode(ChannelBuffer body, int version)
        {
            ByteBuffer b = CBUtil.readValue(body);
            byte[] token = new byte[b.remaining()];
            b.get(token);
            return new AuthChallenge(token);
        }

        public void encode(AuthChallenge challenge, ChannelBuffer dest, int version)
        {
            CBUtil.writeValue(challenge.token, dest);
        }

        public int encodedSize(AuthChallenge challenge, int version)
        {
            return CBUtil.sizeOfValue(challenge.token);
        }
    };

    private byte[] token;

    public AuthChallenge(byte[] token)
    {
        super(Message.Type.AUTH_CHALLENGE);
        this.token = token;
    }

    public byte[] getToken()
    {
        return token;
    }
}
