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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.junit.Test;

import com.codefollower.lealone.omid.replication.Zipper;
import com.codefollower.lealone.omid.tso.TSOMessage;
import com.codefollower.lealone.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CleanedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;

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
