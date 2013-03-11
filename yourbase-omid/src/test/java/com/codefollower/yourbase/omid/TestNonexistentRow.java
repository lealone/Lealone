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

package com.codefollower.yourbase.omid;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.codefollower.yourbase.omid.client.TransactionManager;
import com.codefollower.yourbase.omid.client.TransactionState;
import com.codefollower.yourbase.omid.client.TransactionalTable;

public class TestNonexistentRow extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestNonexistentRow.class);

    @Test
    public void testMultiPutSameRow() throws Exception {
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TransactionalTable table1 = new TransactionalTable(hbaseConf, TEST_TABLE);

            int num = 10;
            TransactionState t = tm.beginTransaction();
            for (int j = 0; j < num; j++) {
                byte[] data = Bytes.toBytes(j);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value"), data);
                table1.put(t, put);
            }
            int key = 15;
            Get g = new Get(Bytes.toBytes(key));
            Result r = table1.get(t, g);

            assertTrue("Found a row that should not exist", r.isEmpty());

            tm.tryCommit(t);
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        }
    }

}