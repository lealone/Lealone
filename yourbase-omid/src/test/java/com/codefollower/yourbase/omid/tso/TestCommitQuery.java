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

package com.codefollower.yourbase.omid.tso;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.codefollower.yourbase.omid.tso.RowKey;
import com.codefollower.yourbase.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.yourbase.omid.tso.messages.CommitQueryRequest;
import com.codefollower.yourbase.omid.tso.messages.CommitQueryResponse;
import com.codefollower.yourbase.omid.tso.messages.CommitRequest;
import com.codefollower.yourbase.omid.tso.messages.CommitResponse;
import com.codefollower.yourbase.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.yourbase.omid.tso.messages.FullAbortRequest;
import com.codefollower.yourbase.omid.tso.messages.TimestampRequest;
import com.codefollower.yourbase.omid.tso.messages.TimestampResponse;

public class TestCommitQuery extends TSOTestBase {

    @Test
    public void testCommitQuery() throws Exception {
        // Disable auto full abort for testing every commit query request case
        clientHandler.setAutoFullAbort(false);

        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new TimestampRequest());
        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);
        assertTrue(tr2.timestamp > tr1.timestamp);

        //
        // Test Commit query of uncommitted transaction
        //
        clientHandler.sendMessage(new CommitQueryRequest(tr2.timestamp, tr1.timestamp));
        CommitQueryResponse cqr1 = clientHandler.receiveMessage(CommitQueryResponse.class);
        assertFalse(cqr1.committed);
        assertFalse(cqr1.retry);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertTrue(cr1.commitTimestamp > tr1.timestamp);
        assertEquals(tr1.timestamp, cr1.startTimestamp);

        //
        // Test Commit query of concurrent committed transaction
        //
        clientHandler.sendMessage(new CommitQueryRequest(tr2.timestamp, tr1.timestamp));
        CommitQueryResponse cqr2 = clientHandler.receiveMessage(CommitQueryResponse.class);
        assertFalse(cqr2.committed);
        assertFalse(cqr2.retry);

        clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        assertFalse(cr2.committed);

        // Queued reports
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveMessage(CommittedTransactionReport.class);
        clientHandler.receiveMessage(AbortedTransactionReport.class);

        TimestampResponse tr3 = clientHandler.receiveMessage(TimestampResponse.class);
        assertTrue(tr3.timestamp > tr2.timestamp);

        state.largestDeletedTimestamp = 1000000;

        //
        // Test Commit query of half aborted transaction
        //
        clientHandler.sendMessage(new CommitQueryRequest(tr3.timestamp, tr2.timestamp));
        CommitQueryResponse cqr3 = clientHandler.receiveMessage(CommitQueryResponse.class);
        assertFalse(cqr3.committed);
        assertFalse(cqr3.retry);

        clientHandler.sendMessage(new FullAbortRequest(tr2.timestamp));

        //
        // Test Commit query of full aborted transaction
        //
        clientHandler.sendMessage(new CommitQueryRequest(tr3.timestamp, tr2.timestamp));
        CommitQueryResponse cqr4 = clientHandler.receiveMessage(CommitQueryResponse.class);
        assertFalse(cqr4.committed);
        assertTrue(cqr4.retry);

        //
        // Test Commit query of committed transaction
        //
        clientHandler.sendMessage(new CommitQueryRequest(tr3.timestamp, tr1.timestamp));
        CommitQueryResponse cqr5 = clientHandler.receiveMessage(CommitQueryResponse.class);
        assertTrue(cqr5.committed);
        assertFalse(cqr5.retry);
    }
}