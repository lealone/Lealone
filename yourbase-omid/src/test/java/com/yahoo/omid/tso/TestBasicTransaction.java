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

package com.yahoo.omid.tso;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CleanedTransactionReport;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;

public class TestBasicTransaction extends TSOTestBase {

    @Test
    public void testConflicts() throws IOException, InterruptedException {
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new TimestampRequest());
        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertEquals(tr1.timestamp, cr1.startTimestamp);

        clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        assertFalse(cr2.committed);

        // TODO fix this. Commiting an already committed value (either failed or commited) should fail
        // FIXME to do this.
        //      clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r2 }));
        //      messageReceived = clientHandler.receiveMessage();
        //      assertThat(messageReceived, is(CommitResponse.class));
        //      CommitResponse cr3 = (CommitResponse) messageReceived;
        //      assertFalse(cr3.committed);

        clientHandler.sendMessage(new TimestampRequest());
        // Pending commit report
        CommittedTransactionReport ctr1 = clientHandler.receiveMessage(CommittedTransactionReport.class);
        assertEquals(cr1.startTimestamp, ctr1.startTimestamp);
        assertEquals(cr1.commitTimestamp, ctr1.commitTimestamp);
        // Aborted transaction report
        AbortedTransactionReport atr = clientHandler.receiveMessage(AbortedTransactionReport.class);
        assertEquals(cr2.startTimestamp, atr.startTimestamp);
        // Full Abort report
        CleanedTransactionReport cltr = clientHandler.receiveMessage(CleanedTransactionReport.class);
        assertEquals(cr2.startTimestamp, cltr.startTimestamp);

        TimestampResponse tr3 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr3.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr3 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr3.committed);
        assertEquals(tr3.timestamp, cr3.startTimestamp);

        clientHandler.sendMessage(new TimestampRequest());

        // Pending commit report
        CommittedTransactionReport ctr3 = clientHandler.receiveMessage(CommittedTransactionReport.class);
        assertEquals(cr3.startTimestamp, ctr3.startTimestamp);
        assertEquals(cr3.commitTimestamp, ctr3.commitTimestamp);

        TimestampResponse tr4 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr4.timestamp, new RowKey[] { r2 }));
        CommitResponse cr4 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr4.committed);
        assertEquals(tr4.timestamp, cr4.startTimestamp);

        // Fetch commit report
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveMessage(CommittedTransactionReport.class);
        clientHandler.receiveMessage(TimestampResponse.class);
    }

}