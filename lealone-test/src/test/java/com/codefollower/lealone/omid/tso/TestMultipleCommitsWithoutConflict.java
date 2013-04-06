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

public class TestMultipleCommitsWithoutConflict extends TSOTestBase {

    @Test
    public void testMultipleCommitsWithoutConflict() throws Exception {
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertTrue(cr1.commitTimestamp > tr1.timestamp);
        assertEquals(tr1.timestamp, cr1.startTimestamp);

        // Queued commit report
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveMessage(CommittedTransactionReport.class);

        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);
        assertTrue(tr2.timestamp > tr1.timestamp);

        clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr2.committed);
        assertTrue(cr2.commitTimestamp > tr2.timestamp);
        assertEquals(tr2.timestamp, cr2.startTimestamp);

        // Queued commit report
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveMessage(CommittedTransactionReport.class);

        TimestampResponse tr3 = clientHandler.receiveMessage(TimestampResponse.class);
        assertTrue(tr3.timestamp > tr1.timestamp);

        clientHandler.sendMessage(new CommitRequest(tr3.timestamp, new RowKey[] { r2 }));
        CommitResponse cr3 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr3.committed);
        assertTrue(cr3.commitTimestamp > tr3.timestamp);
        assertEquals(tr3.timestamp, cr3.startTimestamp);
    }

}