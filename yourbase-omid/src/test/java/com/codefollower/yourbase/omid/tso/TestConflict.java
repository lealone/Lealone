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
import com.codefollower.yourbase.omid.tso.messages.CommitRequest;
import com.codefollower.yourbase.omid.tso.messages.CommitResponse;
import com.codefollower.yourbase.omid.tso.messages.TimestampRequest;
import com.codefollower.yourbase.omid.tso.messages.TimestampResponse;

public class TestConflict extends TSOTestBase {

    @Test
    public void testConflict() throws Exception {
        clientHandler.sendMessage(new TimestampRequest());
        clientHandler.receiveBootstrap();
        TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

        clientHandler.sendMessage(new TimestampRequest());
        TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);
        assertTrue(tr2.timestamp > tr1.timestamp);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1 }));
        CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
        assertTrue(cr1.committed);
        assertTrue(cr1.commitTimestamp > tr1.timestamp);
        assertEquals(tr1.timestamp, cr1.startTimestamp);

        clientHandler.sendMessage(new CommitRequest(tr1.timestamp, new RowKey[] { r1, r2 }));
        CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
        assertFalse(cr2.committed);
    }

}