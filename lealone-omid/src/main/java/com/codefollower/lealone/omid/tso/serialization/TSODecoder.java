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

package com.codefollower.lealone.omid.tso.serialization;

import java.io.EOFException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import com.codefollower.lealone.omid.replication.Zipper;
import com.codefollower.lealone.omid.tso.TSOMessage;
import com.codefollower.lealone.omid.tso.messages.AbortRequest;
import com.codefollower.lealone.omid.tso.messages.CommitQueryRequest;
import com.codefollower.lealone.omid.tso.messages.CommitQueryResponse;
import com.codefollower.lealone.omid.tso.messages.CommitRequest;
import com.codefollower.lealone.omid.tso.messages.CommitResponse;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.FullAbortRequest;
import com.codefollower.lealone.omid.tso.messages.LargestDeletedTimestampReport;
import com.codefollower.lealone.omid.tso.messages.TimestampRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampResponse;

public class TSODecoder extends FrameDecoder {
    private static final Log LOG = LogFactory.getLog(TSODecoder.class);

    private Zipper zipper;

    public TSODecoder(Zipper zipper) {
        this.zipper = zipper;
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buf) throws Exception {
        // Mark the current buffer position before any reading
        // because the whole frame might not be in the buffer yet.
        // We will reset the buffer position to the marked position if
        // there's not enough bytes in the buffer.
        buf.markReaderIndex();

        TSOMessage msg;
        try {
            if (zipper != null) {
                msg = zipper.decodeMessage(buf);
                LOG.debug("Zipper returned " + msg);
                if (msg != null) {
                    return msg;
                }
                buf.resetReaderIndex();
            }
            byte type = buf.readByte();
            if (LOG.isTraceEnabled()) {
                LOG.trace("Decoding message : " + type);
            }
            switch (type) {
            case TSOMessage.TimestampRequest:
                msg = new TimestampRequest();
                break;
            case TSOMessage.TimestampResponse:
                msg = new TimestampResponse();
                break;
            case TSOMessage.CommitRequest:
                msg = new CommitRequest();
                break;
            case TSOMessage.CommitResponse:
                msg = new CommitResponse();
                break;
            case TSOMessage.CommitQueryRequest:
                msg = new CommitQueryRequest();
                break;
            case TSOMessage.CommitQueryResponse:
                msg = new CommitQueryResponse();
                break;
            case TSOMessage.CommittedTransactionReport:
                msg = new CommittedTransactionReport();
                break;
            case TSOMessage.LargestDeletedTimestampReport:
                msg = new LargestDeletedTimestampReport();
                break;
            case TSOMessage.AbortRequest:
                msg = new AbortRequest();
                break;
            case TSOMessage.FullAbortReport:
                msg = new FullAbortRequest();
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
