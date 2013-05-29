package com.codefollower.lealone.omid;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.omid.client.RowKeyFamily;
import com.codefollower.lealone.omid.client.SyncAbortCompleteCallback;
import com.codefollower.lealone.omid.transaction.Transaction;
import com.codefollower.lealone.omid.transaction.TransactionManager;


public class TSOHandlerTest {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        TransactionManager tm = new TransactionManager(conf);
        Transaction ts = tm.begin();

        Put put = new Put(Bytes.toBytes("2002"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("2002"));
        put(ts, put);

        put = new Put(Bytes.toBytes("2003"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("2003"));
        put(ts, put);

        tm.commit(ts);
        //System.out.println(ts.tsoclient.validRead(ts.getCommitTimestamp(), ts.getStartTimestamp()));
        //System.out.println(ts.tsoclient.validRead(8, ts.getStartTimestamp()));

        TransactionManager.tsoclient.abort(ts.getStartTimestamp());
        System.out.println(TransactionManager.tsoclient.validRead(ts.getStartTimestamp() - 1, ts.getStartTimestamp()));

        SyncAbortCompleteCallback c = new SyncAbortCompleteCallback();
        TransactionManager.tsoclient.completeAbort(ts.getStartTimestamp(), c);
        c.await();
        //TransactionManager.close();
    }

    public static void put(Transaction transactionState, Put put) throws IOException, IllegalArgumentException {
        final long startTimestamp = transactionState.getStartTimestamp();
        // create put with correct ts
        final Put tsput = new Put(put.getRow(), startTimestamp); //把事务的开始时间戳放到Put里
        Map<byte[], List<KeyValue>> kvs = put.getFamilyMap();
        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                tsput.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), startTimestamp, kv.getValue()));
            }
        }

        // should add the table as well
        transactionState.addRow(new RowKeyFamily(tsput.getRow(), Bytes.toBytes("mytable"), tsput.getFamilyMap()));
    }
}
