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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.codefollower.lealone.omid.tso.RowKey;
import com.codefollower.lealone.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CleanedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.CommitQueryRequest;
import com.codefollower.lealone.omid.tso.messages.CommitRequest;
import com.codefollower.lealone.omid.tso.messages.CommitResponse;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.FullAbortRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampResponse;

public class TestCommitAbortedReport extends TSOTestBase {

    @Test
    public void testCommitAbortedReport() throws Exception {
        secondClientHandler.setAutoFullAbort(false);

        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        secondClientHandler.sendMessage(new TimestampRequest());
        secondClientHandler.receiveBootstrap();
        TimestampResponse tr2 = secondClientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertTrue(cr1.commitTimestamp > tr1.timestamp);
        assertEquals(tr1.timestamp, cr1.startTimestamp);

        secondClientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr2 = secondClientHandler.receiveMessage(CommitResponse.class);
        assertEquals(tr2.timestamp, cr2.startTimestamp);
        assertFalse(cr2.committed);

        clientHandler.sendMessage(new TimestampRequest());
        CommittedTransactionReport ctr2 = clientHandler.receiveMessage(CommittedTransactionReport.class);
        assertEquals(cr1.commitTimestamp, ctr2.commitTimestamp);
        assertEquals(cr1.startTimestamp, ctr2.startTimestamp);

        //
        // Half Aborted Transaction Report
        //
        AbortedTransactionReport atr1 = clientHandler.receiveMessage(AbortedTransactionReport.class);
        assertEquals(tr2.timestamp, atr1.startTimestamp);

        clientHandler.receiveMessage(TimestampResponse.class);

        secondClientHandler.sendMessage(new TimestampRequest());
        CommittedTransactionReport ctr3 = secondClientHandler.receiveMessage(CommittedTransactionReport.class);
        assertEquals(cr1.commitTimestamp, ctr3.commitTimestamp);
        assertEquals(cr1.startTimestamp, ctr3.startTimestamp);

        //
        // Half Aborted Transaction Report
        //
        AbortedTransactionReport atr2 = secondClientHandler.receiveMessage(AbortedTransactionReport.class);
        assertEquals(tr2.timestamp, atr2.startTimestamp);

        secondClientHandler.receiveMessage(TimestampResponse.class);

        secondClientHandler.sendMessage(new FullAbortRequest(tr2.timestamp));

        // Let the TSO receive and process the FullAbortReport
        secondClientHandler.sendMessage(new CommitQueryRequest(0, 0));
        secondClientHandler.receiveMessage();

        //
        // Cleaned Transaction Report
        //
        clientHandler.sendMessage(new TimestampRequest());
        CleanedTransactionReport cltr1 = clientHandler.receiveMessage(CleanedTransactionReport.class);
        assertEquals(tr2.timestamp, cltr1.startTimestamp);

        //
        // Cleaned Transaction Report
        //
        secondClientHandler.sendMessage(new TimestampRequest());
        CleanedTransactionReport cltr2 = secondClientHandler.receiveMessage(CleanedTransactionReport.class);
        assertEquals(tr2.timestamp, cltr2.startTimestamp);
    }

}