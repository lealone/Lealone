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
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.codefollower.lealone.omid.tso.RowKey;
import com.codefollower.lealone.omid.tso.messages.CommitRequest;
import com.codefollower.lealone.omid.tso.messages.CommitResponse;
import com.codefollower.lealone.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.lealone.omid.tso.messages.TimestampRequest;
import com.codefollower.lealone.omid.tso.messages.TimestampResponse;

public class TestCommitReport extends TSOTestBase {

    @Test
    public void testCommitReport() throws Exception {
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertTrue(cr1.commitTimestamp > tr1.timestamp);
        assertEquals(tr1.timestamp, cr1.startTimestamp);

        secondClientHandler.sendMessage(new TimestampRequest());
        secondClientHandler.receiveBootstrap();
        TimestampResponse tr2 = secondClientHandler.receiveMessage(TimestampResponse.class);

        secondClientHandler.sendMessage(new CommitRequest(tr2.timestamp));
        CommitResponse cr2 = secondClientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr2.committed);
        assertTrue(cr2.commitTimestamp > tr2.timestamp);
        assertEquals(tr2.timestamp, cr2.startTimestamp);

        clientHandler.sendMessage(new TimestampRequest());
        // Txn 2 was a readonly transaction, so no committed info received
        clientHandler.receiveMessage(TimestampResponse.class);

        secondClientHandler.sendMessage(new TimestampRequest());
        TimestampResponse tr3 = secondClientHandler.receiveMessage(TimestampResponse.class);

        secondClientHandler.sendMessage(new CommitRequest(tr3.timestamp, new RowKey[] { r1 }));
        CommitResponse cr3 = secondClientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr3.committed);
        assertTrue(cr3.commitTimestamp > tr3.timestamp);
        assertEquals(tr3.timestamp, cr3.startTimestamp);

        clientHandler.sendMessage(new TimestampRequest());
        // Txn 3 did write, so we must receive CommittedReport
        CommittedTransactionReport ctr = clientHandler.receiveMessage(CommittedTransactionReport.class);
        assertEquals(cr3.startTimestamp, ctr.startTimestamp);
        assertEquals(cr3.commitTimestamp, ctr.commitTimestamp);

        clientHandler.receiveMessage(TimestampResponse.class);
    }

}