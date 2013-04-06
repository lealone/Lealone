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

package com.codefollower.lealone.omid.tso;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.codefollower.lealone.omid.tso.RowKey;
import com.codefollower.lealone.omid.tso.messages.CommitRequest;
import com.codefollower.lealone.omid.tso.messages.CommitResponse;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.TimestampRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampResponse;

public class TestPersistence extends TSOTestBase {
    private static final Log LOG = LogFactory.getLog(TestPersistence.class);

    @Override
    protected boolean recoveryEnabled() {
        return true;
    }

    @Test
    public void testCommit() throws Exception {

        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);

        clientHandler.sendMessage(new TimestampRequest());
        // Pending commit report
        CommittedTransactionReport ctr1 = clientHandler.receiveMessage(CommittedTransactionReport.class);
        assertEquals(cr1.startTimestamp, ctr1.startTimestamp);
        assertEquals(cr1.commitTimestamp, ctr1.commitTimestamp);

        tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1, r2 }));
        cr1 = clientHandler.receiveMessage(CommitResponse.class);

        LOG.info("Going to shut down TSO");
        teardownTSO();

        LOG.info("Going to restart TSO");
        setupTSO();

        LOG.info("TSO started, going to process transactions");
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        LOG.info("Timestamps: start " + tr2.timestamp + ", commit" + cr2.startTimestamp);
        assertTrue(cr2.committed);
        assertTrue(cr2.commitTimestamp > tr2.timestamp);
        assertEquals(tr2.timestamp, cr2.startTimestamp);
        assertThat(tr2.timestamp, is(greaterThan(tr1.timestamp)));

    }

    @Test
    public void testBigLog() throws Exception {
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();

        for (int i = 0; i < 10000; ++i) {
            Object msg;
            while (!((msg = clientHandler.receiveMessage()) instanceof TimestampResponse))
                // iterate until we get a TimestampResponse
                ;

            TimestampResponse tr1 = (TimestampResponse) msg;

            clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1, r2 }));
            clientHandler.receiveMessage(CommitResponse.class);
            clientHandler.sendMessage(new TimestampRequest());
        }
        clientHandler.clearMessages();

        LOG.info("Going to shut down TSO");
        teardownTSO();

        LOG.info("Going to restart TSO");
        setupTSO();

        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        clientHandler.receiveMessage(TimestampResponse.class);
    }

}