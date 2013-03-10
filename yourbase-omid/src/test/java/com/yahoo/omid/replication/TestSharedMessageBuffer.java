package com.yahoo.omid.replication;

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

import com.yahoo.omid.replication.SharedMessageBuffer.ReadingBuffer;
import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CleanedTransactionReport;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.LargestDeletedTimestampReport;
import com.yahoo.omid.tso.serialization.TSODecoder;

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
            smb.writeLargestIncrease(firstTS);
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
