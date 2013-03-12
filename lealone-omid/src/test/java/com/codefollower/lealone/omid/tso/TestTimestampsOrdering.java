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

public class TestTimestampsOrdering extends TSOTestBase {

    @Test
    public void testTimestampsOrdering() throws Exception {
        long timestamp;

        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);
        timestamp = tr1.timestamp;

        clientHandler.sendMessage(new TimestampRequest());
        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);
        assertEquals(++timestamp, tr2.timestamp);

        clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertEquals(tr2.timestamp, cr1.startTimestamp);
        assertEquals(++timestamp, cr1.commitTimestamp);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r2 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr2.committed);
        assertEquals(tr1.timestamp, cr2.startTimestamp);
        assertEquals(++timestamp, cr2.commitTimestamp);

        {
            clientHandler.sendMessage(new TimestampRequest());
            CommittedTransactionReport ctr1 = clientHandler.receiveMessage(CommittedTransactionReport.class);
            assertEquals(cr1.startTimestamp, ctr1.startTimestamp);
            assertEquals(cr1.commitTimestamp, ctr1.commitTimestamp);

            CommittedTransactionReport ctr2 = clientHandler.receiveMessage(CommittedTransactionReport.class);
            assertEquals(cr2.startTimestamp, ctr2.startTimestamp);
            assertEquals(cr2.commitTimestamp, ctr2.commitTimestamp);

            TimestampResponse tr3 = clientHandler.receiveMessage(TimestampResponse.class);
            assertEquals(++timestamp, tr3.timestamp);
        }
    }
}