/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso.serialization;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;

import com.yahoo.omid.replication.ZipperState;
import com.yahoo.omid.tso.BufferPool;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortRequest;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitQueryRequest;
import com.yahoo.omid.tso.messages.CommitQueryResponse;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortRequest;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

public class TSOEncoder extends OneToOneEncoder {

    //just override decode method
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
        } else if (msg instanceof CommitRequest) {
            objWrapper.writeByte(TSOMessage.CommitRequest);
        } else if (msg instanceof CommitResponse) {
            objWrapper.writeByte(TSOMessage.CommitResponse);
        } else if (msg instanceof AbortRequest) {
            objWrapper.writeByte(TSOMessage.AbortRequest);
        } else if (msg instanceof FullAbortRequest) {
            objWrapper.writeByte(TSOMessage.FullAbortReport);
        } else if (msg instanceof CommitQueryRequest) {
            objWrapper.writeByte(TSOMessage.CommitQueryRequest);
        } else if (msg instanceof CommitQueryResponse) {
            objWrapper.writeByte(TSOMessage.CommitQueryResponse);
        } else if (msg instanceof AbortedTransactionReport) {
            objWrapper.writeByte(TSOMessage.AbortedTransactionReport);
        } else if (msg instanceof CommittedTransactionReport) {
            objWrapper.writeByte(TSOMessage.CommittedTransactionReport);
        } else if (msg instanceof LargestDeletedTimestampReport) {
            objWrapper.writeByte(TSOMessage.LargestDeletedTimestampReport);
        } else if (msg instanceof ZipperState) {
            objWrapper.writeByte(TSOMessage.ZipperState);
        } else
            throw new Exception("Wrong obj");
        ((TSOMessage) msg).writeObject(objWrapper);
        ChannelBuffer result = ChannelBuffers.wrappedBuffer(buffer.toByteArray());
        BufferPool.pushBuffer(buffer);
        return result;
    }
}
