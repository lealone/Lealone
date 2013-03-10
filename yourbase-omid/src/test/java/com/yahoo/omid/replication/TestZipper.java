package com.yahoo.omid.replication;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import com.yahoo.omid.tso.TSOMessage;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CleanedTransactionReport;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;

public class TestZipper {
    @Test
    public void testCommits() {
        Zipper zipper = new Zipper();
        Zipper dezipper = new Zipper();
        ChannelBuffer buffer = ChannelBuffers.buffer(1024 * 1024);
        for (int startDiff = -16384; startDiff < 16384; startDiff++) {
            for (int commitDiff = -512; commitDiff < 512; commitDiff++) {
                long st = zipper.lastStartTimestamp + startDiff;
                long ct = zipper.lastCommitTimestamp + commitDiff;
                buffer.clear();
                zipper.encodeCommit(buffer, st, ct);
                TSOMessage msg = dezipper.decodeMessage(buffer);
                assertThat(msg, is(CommittedTransactionReport.class));
                CommittedTransactionReport ctr = (CommittedTransactionReport) msg;
                assertThat("startDiff: " + startDiff + " commitDiff: " + commitDiff, ctr.commitTimestamp, is(ct));
                assertThat("startDiff: " + startDiff + " commitDiff: " + commitDiff, ctr.startTimestamp, is(st));
            }
        }
    }

    @Test
    public void testAborts() {
        Zipper zipper = new Zipper();
        Zipper dezipper = new Zipper();
        ChannelBuffer buffer = ChannelBuffers.buffer(1024 * 1024);
        for (int tsDiff = -1024 * 1024; tsDiff < 1024 * 1024; tsDiff++) {
            long st = zipper.lastStartTimestamp + tsDiff;
            buffer.clear();
            zipper.encodeHalfAbort(buffer, st);
            zipper.encodeFullAbort(buffer, st);
            TSOMessage msg = dezipper.decodeMessage(buffer);
            assertThat(msg, is(AbortedTransactionReport.class));
            AbortedTransactionReport atr = (AbortedTransactionReport) msg;
            assertThat("startDiff: " + tsDiff, atr.startTimestamp, is(st));
            msg = dezipper.decodeMessage(buffer);
            assertThat(msg, is(CleanedTransactionReport.class));
            CleanedTransactionReport ctr = (CleanedTransactionReport) msg;
            assertThat("startDiff: " + tsDiff, ctr.startTimestamp, is(st));
        }
    }
}
