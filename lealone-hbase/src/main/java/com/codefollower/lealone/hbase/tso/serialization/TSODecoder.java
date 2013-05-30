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

import java.io.EOFException;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

import com.codefollower.lealone.hbase.tso.messages.TSOMessage;
import com.codefollower.lealone.hbase.tso.messages.TimestampRequest;
import com.codefollower.lealone.hbase.tso.messages.TimestampResponse;

public class TSODecoder extends FrameDecoder {

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Mark the current buffer position before any reading
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();

        TSOMessage msg;
        try {
            byte type = buf.readByte();
            switch (type) {
            case TSOMessage.TimestampRequest:
                msg = new TimestampRequest();
                break;
            case TSOMessage.TimestampResponse:
                msg = new TimestampResponse();
                break;
            default:
                throw new Exception("Wrong type " + type + " (" + Integer.toHexString(type) + ") " + buf.toString().length());
            }
            msg.readObject(buf);
        } catch (IndexOutOfBoundsException e) {
            // Not enough byte in the buffer, reset to the start for the next try
            buf.resetReaderIndex();
            return null;
        } catch (EOFException e) {
            // Not enough byte in the buffer, reset to the start for the next try
            buf.resetReaderIndex();
            return null;
        }

        return msg;
    }
}
