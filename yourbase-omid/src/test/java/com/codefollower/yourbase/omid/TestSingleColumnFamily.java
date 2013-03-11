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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.codefollower.yourbase.omid.client.TransactionManager;
import com.codefollower.yourbase.omid.client.TransactionState;
import com.codefollower.yourbase.omid.client.TransactionalTable;

public class TestSingleColumnFamily extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestSingleColumnFamily.class);

    @Test
    public void testSingleColumnFamily() throws Exception {
        TransactionManager tm = new TransactionManager(hbaseConf);
        TransactionalTable table1 = new TransactionalTable(hbaseConf, TEST_TABLE);
        int num = 10;
        TransactionState t = tm.beginTransaction();
        for (int j = 0; j < num; j++) {
            byte[] data = Bytes.toBytes(j);
            Put put = new Put(data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1"), data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2"), data);
            table1.put(t, put);
        }
        //tm.tryCommit(t);
        //t=tm.beginTransaction(); //Visible if in a different transaction
        Scan s = new Scan();
        ResultScanner res = table1.getScanner(t, s);
        Result rr;
        int count = 0;
        while ((rr = res.next()) != null) {
            int tmp1 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1")));
            int tmp2 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2")));
            LOG.info("RES:" + tmp1 + ";" + tmp2);
            count++;
        }
        assertTrue("Can't see puts. I should see " + num + " but I see " + count, num == count);

        tm.tryCommit(t);
        t = tm.beginTransaction();

        for (int j = 0; j < num / 2; j++) {
            byte[] data = Bytes.toBytes(j);
            byte[] ndata = Bytes.toBytes(j * 10);
            Put put = new Put(data);
            put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2"), ndata);
            table1.put(t, put);
        }
        tm.tryCommit(t);
        t = tm.beginTransaction();
        s = new Scan();
        res = table1.getScanner(t, s);
        count = 0;
        int modified = 0, notmodified = 0;
        while ((rr = res.next()) != null) {
            int tmp1 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value1")));
            int tmp2 = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes("value2")));
            LOG.info("RES:" + tmp1 + ";" + tmp2);
            count++;

            if (tmp2 == Bytes.toInt(rr.getRow()) * 10) {
                modified++;
            } else {
                notmodified++;
            }
            if (count == 8) {
                System.out.println("stop");
            }
        }
        assertTrue("Can't see puts. I should see " + num + " but I see " + count, num == count);
        assertTrue("Half of rows should equal row id, half not (" + modified + ", " + notmodified + ")", modified == notmodified
                && notmodified == (num / 2));

        tm.tryCommit(t);
        LOG.info("End commiting");
        table1.close();
    }
}