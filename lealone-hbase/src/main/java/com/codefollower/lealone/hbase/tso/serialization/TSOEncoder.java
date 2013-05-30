/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.codefollower.lealone.hbase.tso.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.codefollower.lealone.hbase.tso.messages.TSOMessage;
import com.codefollower.lealone.hbase.tso.messages.TimestampRequest;
import com.codefollower.lealone.hbase.tso.messages.TimestampResponse;

public class TSOEncoder extends OneToOneEncoder {

    //just override decode method
    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        ByteArrayOutputStream buffer = BufferPool.getBuffer();
        buffer.reset();
        DataOutputStream objWrapper = new DataOutputStream(buffer);
        if (msg instanceof ChannelBuffer) {
            return msg;
        } else if (msg instanceof TimestampRequest) {
            objWrapper.writeByte(TSOMessage.TimestampRequest);
        } else if (msg instanceof TimestampResponse) {
            objWrapper.writeByte(TSOMessage.TimestampResponse);
        } else
            throw new Exception("Wrong obj");
        ((TSOMessage) msg).writeObject(objWrapper);
        ChannelBuffer result = ChannelBuffers.wrappedBuffer(buffer.toByteArray());
        BufferPool.pushBuffer(buffer);
        return result;
    }
}
