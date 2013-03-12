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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.codefollower.lealone.omid.client.TransactionManager;
import com.codefollower.lealone.omid.client.TransactionState;
import com.codefollower.lealone.omid.client.TransactionalTable;

public class TestUpdateScan extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestUpdateScan.class);
    private static final String TEST_COL = "value";

    @Test
    public void testGet() throws Exception {
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TransactionalTable table = new TransactionalTable(hbaseConf, TEST_TABLE);
            TransactionState t = tm.beginTransaction();
            int[] lInts = new int[] { 100, 243, 2342, 22, 1, 5, 43, 56 };
            for (int i = 0; i < lInts.length; i++) {
                byte[] data = Bytes.toBytes(lInts[i]);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }
            int startKeyValue = lInts[3];
            int stopKeyValue = lInts[3];
            byte[] startKey = Bytes.toBytes(startKeyValue);
            byte[] stopKey = Bytes.toBytes(stopKeyValue);
            Get g = new Get(startKey);
            Result r = table.get(t, g);
            if (!r.isEmpty()) {
                int tmp = Bytes.toInt(r.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL)));
                LOG.info("Result:" + tmp);
                assertTrue("Bad value, should be " + startKeyValue + " but is " + tmp, tmp == startKeyValue);
            } else {
                fail("Bad result");
            }
            tm.tryCommit(t);

            Scan s = new Scan(startKey);
            CompareFilter.CompareOp op = CompareFilter.CompareOp.LESS_OR_EQUAL;
            RowFilter toFilter = new RowFilter(op, new BinaryPrefixComparator(stopKey));
            boolean startInclusive = true;
            if (!startInclusive) {
                FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                filters.addFilter(new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(startKey)));
                filters.addFilter(new WhileMatchFilter(toFilter));
                s.setFilter(filters);
            } else {
                s.setFilter(new WhileMatchFilter(toFilter));
            }
            t = tm.beginTransaction();
            ResultScanner res = table.getScanner(t, s);
            Result rr;
            int count = 0;
            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertEquals("Count is wrong", 1, count);
            LOG.info("Rows found " + count);
            tm.tryCommit(t);
            table.close();
        } catch (Exception e) {
            LOG.error("Exception in test", e);
        }
    }

    @Test
    public void testScan() throws Exception {
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TransactionalTable table = new TransactionalTable(hbaseConf, TEST_TABLE);
            TransactionState t = tm.beginTransaction();
            int[] lInts = new int[] { 100, 243, 2342, 22, 1, 5, 43, 56 };
            for (int i = 0; i < lInts.length; i++) {
                byte[] data = Bytes.toBytes(lInts[i]);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }

            Scan s = new Scan();
            ResultScanner res = table.getScanner(t, s);
            Result rr;
            int count = 0;
            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertTrue("Count should be " + lInts.length + " but is " + count, count == lInts.length);
            LOG.info("Rows found " + count);

            tm.tryCommit(t);

            t = tm.beginTransaction();
            res = table.getScanner(t, s);
            count = 0;
            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertTrue("Count should be " + lInts.length + " but is " + count, count == lInts.length);
            LOG.info("Rows found " + count);
            tm.tryCommit(t);
            table.close();
        } catch (Exception e) {
            LOG.error("Exception in test", e);
        }
    }

    @Test
    public void testScanUncommitted() throws Exception {
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TransactionalTable table = new TransactionalTable(hbaseConf, TEST_TABLE);
            TransactionState t = tm.beginTransaction();
            int[] lIntsA = new int[] { 100, 243, 2342, 22, 1, 5, 43, 56 };
            for (int i = 0; i < lIntsA.length; i++) {
                byte[] data = Bytes.toBytes(lIntsA[i]);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }
            tm.tryCommit(t);

            TransactionState tu = tm.beginTransaction();
            int[] lIntsB = new int[] { 105, 24, 4342, 32, 7, 3, 30, 40 };
            for (int i = 0; i < lIntsB.length; i++) {
                byte[] data = Bytes.toBytes(lIntsB[i]);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(tu, put);
            }

            t = tm.beginTransaction();
            int[] lIntsC = new int[] { 109, 224, 242, 2, 16, 59, 23, 26 };
            for (int i = 0; i < lIntsC.length; i++) {
                byte[] data = Bytes.toBytes(lIntsC[i]);
                Put put = new Put(data);
                put.add(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL), data);
                table.put(t, put);
            }
            tm.tryCommit(t);

            t = tm.beginTransaction();
            Scan s = new Scan();
            ResultScanner res = table.getScanner(t, s);
            Result rr;
            int count = 0;

            while ((rr = res.next()) != null) {
                int iTmp = Bytes.toInt(rr.getValue(Bytes.toBytes(TEST_FAMILY), Bytes.toBytes(TEST_COL)));
                LOG.info("Result: " + iTmp);
                count++;
            }
            assertTrue("Count should be " + (lIntsA.length * lIntsC.length) + " but is " + count, count == lIntsA.length
                    + lIntsC.length);
            LOG.info("Rows found " + count);
            tm.tryCommit(t);
            table.close();
        } catch (Exception e) {
            LOG.error("Exception in test", e);
        }
    }

}