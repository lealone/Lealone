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

package com.codefollower.lealone.omid.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelDownstreamHandler;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.SimpleChannelDownstreamHandler;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
import org.jboss.netty.handler.codec.embedder.EncoderEmbedder;
import org.junit.Test;

import com.codefollower.lealone.omid.replication.SharedMessageBuffer;
import com.codefollower.lealone.omid.replication.Zipper;
import com.codefollower.lealone.omid.replication.SharedMessageBuffer.ReadingBuffer;
import com.codefollower.lealone.omid.tso.TSOMessage;
import com.codefollower.lealone.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CleanedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.LargestDeletedTimestampReport;
import com.codefollower.lealone.omid.tso.serialization.TSODecoder;

public class TestSharedMessageBuffer {

    @Test
    public void testShortFlushes() {
        final int ITERATIONS = 1000000;
        SharedMessageBuffer smb = new SharedMessageBuffer();
        final DecoderEmbedder<TSOMessage> decoder = new DecoderEmbedder<TSOMessage>(new TSODecoder(new Zipper()));
        ChannelDownstreamHandler handler = new SimpleChannelDownstreamHandler();
        EncoderEmbedder<ChannelBuffer> encoder = new EncoderEmbedder<ChannelBuffer>(handler);
        ChannelHandlerContext ctx = encoder.getPipeline().getContext(handler);
        Channel channel = ctx.getChannel();
        ReadingBuffer rb = smb.getReadingBuffer(ctx);
        rb.initializeIndexes();
        Random rand = new Random();

        Deque<TSOMessage> expectedMessages = new ArrayDeque<TSOMessage>();
        int checked = 0;

        // Write one message to the shared buffer and read it as a client
        for (int i = 0; i < ITERATIONS; ++i) {
            writeRandomMessage(smb, rand, expectedMessages);

            ChannelFuture future = Channels.succeededFuture(channel);
            ChannelBuffer buffer = rb.flush(future);
            Channels.write(ctx, future, buffer);

            forwardMessages(encoder, decoder);

            checked += checkExpectedMessage(decoder, expectedMessages);
        }

        assertTrue("Some messages weren't consumed", expectedMessages.isEmpty());
        assertEquals("Didn't check the generated number of messages", ITERATIONS, checked);
    }

    @Test
    public void testLongFlushes() {
        final int ITERATIONS = 1000000;
        SharedMessageBuffer smb = new SharedMessageBuffer();
        final DecoderEmbedder<TSOMessage> decoder = new DecoderEmbedder<TSOMessage>(new TSODecoder(new Zipper()));
        ChannelDownstreamHandler handler = new SimpleChannelDownstreamHandler();
        EncoderEmbedder<ChannelBuffer> encoder = new EncoderEmbedder<ChannelBuffer>(handler);
        ChannelHandlerContext ctx = encoder.getPipeline().getContext(handler);
        Channel channel = ctx.getChannel();
        ReadingBuffer rb = smb.getReadingBuffer(ctx);
        rb.initializeIndexes();
        Random rand = new Random();

        Deque<TSOMessage> expectedMessages = new ArrayDeque<TSOMessage>();

        for (int i = 0; i < ITERATIONS; ++i) {
            writeRandomMessage(smb, rand, expectedMessages);
        }

        // Flush the remaining messages
        ChannelFuture future = Channels.succeededFuture(channel);
        ChannelBuffer buffer = rb.flush(future);
        Channels.write(ctx, future, buffer);

        forwardMessages(encoder, decoder);

        int checked = checkExpectedMessage(decoder, expectedMessages);

        assertTrue("Some messages weren't consumed", expectedMessages.isEmpty());
        assertEquals("Didn't check the generated number of messages", ITERATIONS, checked);
    }

    private int checkExpectedMessage(DecoderEmbedder<TSOMessage> decoder, Deque<TSOMessage> expectedMessages) {
        int checked = 0;
        while (!expectedMessages.isEmpty()) {
            TSOMessage expected = expectedMessages.poll();
            TSOMessage actual = decoder.poll();
            assertEquals("Read message didn't correspond to written message", expected, actual);
            checked++;
        }
        return checked;
    }

    private void writeRandomMessage(SharedMessageBuffer smb, Random rand, Deque<TSOMessage> expectedMessages) {
        long firstTS = rand.nextLong();
        long secondTS = rand.nextLong();
        switch (rand.nextInt(4)) {
        case 0:
            smb.writeCommit(firstTS, secondTS);
            expectedMessages.add(new CommittedTransactionReport(firstTS, secondTS));
            break;
        case 1:
            smb.writeFullAbort(firstTS);
            expectedMessages.add(new CleanedTransactionReport(firstTS));
            break;
        case 2:
            smb.writeHalfAbort(firstTS);
            expectedMessages.add(new AbortedTransactionReport(firstTS));
            break;
        case 3:
            smb.writeLargestDeletedTimestamp(firstTS);
            expectedMessages.add(new LargestDeletedTimestampReport(firstTS));
            break;
        }
    }

    private void forwardMessages(EncoderEmbedder<ChannelBuffer> encoder, DecoderEmbedder<TSOMessage> decoder) {
        ChannelBuffer buffer;
        while ((buffer = encoder.poll()) != null) {
            decoder.offer(buffer);
        }
    }
}
