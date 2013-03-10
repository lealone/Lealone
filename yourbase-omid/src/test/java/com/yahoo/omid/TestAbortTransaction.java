package com.yahoo.omid;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.yahoo.omid.client.TransactionManager;
import com.yahoo.omid.client.TransactionState;
import com.yahoo.omid.client.TransactionalTable;

public class TestAbortTransaction extends OmidTestBase {
    private static final Log LOG = LogFactory.getLog(TestAbortTransaction.class);

    @Test
    public void runTestInterleaveScan() throws Exception {
        try {
            TransactionManager tm = new TransactionManager(hbaseConf);
            TransactionalTable tt = new TransactionalTable(hbaseConf, TEST_TABLE);

            TransactionState t1 = tm.beginTransaction();
            LOG.info("Transaction created " + t1);

            byte[] fam = Bytes.toBytes(TEST_FAMILY);
            byte[] col = Bytes.toBytes("testdata");
            byte[] data1 = Bytes.toBytes("testWrite-1");
            byte[] data2 = Bytes.toBytes("testWrite-2");

            byte[] startrow = Bytes.toBytes("test-scan" + 0);
            byte[] stoprow = Bytes.toBytes("test-scan" + 9);
            byte[] modrow = Bytes.toBytes("test-scan" + 3);
            for (int i = 0; i < 10; i++) {
                byte[] row = Bytes.toBytes("test-scan" + i);

                Put p = new Put(row);
                p.add(fam, col, data1);
                tt.put(t1, p);
            }
            tm.tryCommit(t1);

            TransactionState t2 = tm.beginTransaction();
            Put p = new Put(modrow);
            p.add(fam, col, data2);
            tt.put(t2, p);

            int modifiedrows = 0;
            ResultScanner rs = tt.getScanner(t2, new Scan().setStartRow(startrow).setStopRow(stoprow).addColumn(fam, col));
            Result r = rs.next();
            while (r != null) {
                if (Bytes.equals(data2, r.getValue(fam, col))) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Modified :" + Bytes.toString(r.getRow()));
                    }
                    modifiedrows++;
                }

                r = rs.next();
            }

            assertTrue("Expected 1 row modified, but " + modifiedrows + " are.", modifiedrows == 1);
            tm.abort(t2);

            TransactionState tscan = tm.beginTransaction();
            rs = tt.getScanner(tscan, new Scan().setStartRow(startrow).setStopRow(stoprow).addColumn(fam, col));
            r = rs.next();
            while (r != null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Scan1 :" + Bytes.toString(r.getRow()) + " => " + Bytes.toString(r.getValue(fam, col)));
                }

                assertTrue("Unexpected value for SI scan " + tscan + ": " + Bytes.toString(r.getValue(fam, col)),
                        Bytes.equals(data1, r.getValue(fam, col)));
                r = rs.next();
            }

        } catch (Exception e) {
            LOG.error("Exception occurred", e);
            throw e;
        }
    }
}