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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.codefollower.yourbase.omid.tso.RowKey;
import com.codefollower.yourbase.omid.tso.messages.AbortedTransactionReport;
import com.codefollower.yourbase.omid.tso.messages.CommitRequest;
import com.codefollower.yourbase.omid.tso.messages.CommitResponse;
import com.codefollower.yourbase.omid.tso.messages.CommittedTransactionReport;
import com.codefollower.yourbase.omid.tso.messages.TimestampRequest;
import com.codefollower.yourbase.omid.tso.messages.TimestampResponse;

public class TestReadAlgorithm extends TSOTestBase {

    @Test
    public void testReadAlgorithm() throws Exception {
        secondClientHandler.sendMessage(new TimestampRequest());
        secondClientHandler.receiveBootstrap();
        TimestampResponse tr1 = secondClientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);
        TimestampResponse tr3 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.setAutoFullAbort(false);
        clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        clientHandler.sendMessage(new CommitRequest(tr3.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        assertFalse(cr2.committed);

        secondClientHandler.sendMessage(new TimestampRequest());
        secondClientHandler.receiveMessage(CommittedTransactionReport.class);
        secondClientHandler.receiveMessage(AbortedTransactionReport.class);
        TimestampResponse tr4 = secondClientHandler.receiveMessage(TimestampResponse.class);

        // Transaction half aborted
        assertFalse(secondClientHandler.validRead(tr3.timestamp, tr4.timestamp));

        // Transaction committed after start timestamp
        assertFalse(secondClientHandler.validRead(tr2.timestamp, tr1.timestamp));

        // Transaction committed before start timestamp
        assertTrue(secondClientHandler.validRead(tr2.timestamp, tr4.timestamp));

    }

}