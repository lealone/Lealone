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

package com.codefollower.lealone.omid;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.codefollower.lealone.omid.transaction.TransactionManager;
import com.codefollower.lealone.omid.transaction.Transaction;
import com.codefollower.lealone.omid.transaction.TTable;

public class TestMultiplePut extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestMultiplePut.class);

    @Test
    public void testMultiPutSameRow() throws Exception {
        try {
            byte[] family = Bytes.toBytes(TEST_FAMILY);
            byte[] col1 = Bytes.toBytes("value1");
            byte[] col2 = Bytes.toBytes("value2");
            TransactionManager tm = new TransactionManager(hbaseConf);
            TTable table1 = new TTable(hbaseConf, TEST_TABLE);
            Transaction t = tm.begin();
            int val = 1000;
            byte[] data = Bytes.toBytes(val);
            Put put1 = new Put(data);
            put1.add(family, col1, data);
            table1.put(t, put1);
            Put put2 = new Put(data);
            put2.add(family, col2, data);
            table1.put(t, put2);
            tm.commit(t);
            table1.close();

            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE), data, family, col1, data));
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE), data, family, col2, data));
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        }
    }

    @Test
    public void testManyManyPut() throws Exception {
        try {
            byte[] family = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("value");

            TransactionManager tm = new TransactionManager(hbaseConf);
            TTable table1 = new TTable(hbaseConf, TEST_TABLE);
            Transaction t = tm.begin();
            int num = 50;
            for (int j = 0; j <= num; j++) {
                byte[] data = Bytes.toBytes(j);
                Put put = new Put(data);
                put.add(family, col, data);
                table1.put(t, put);
            }
            tm.commit(t);
            table1.close();

            byte[] data = Bytes.toBytes(0);
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE), data, family, col, data));
            data = Bytes.toBytes(num / 2);
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE), data, family, col, data));
            data = Bytes.toBytes(num);
            assertTrue("Invalid value in table", verifyValue(Bytes.toBytes(TEST_TABLE), data, family, col, data));
        } catch (Exception e) {
            LOG.error("Exception in test", e);
            throw e;
        }
    }
}