/*
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codefollower.lealone.omid.transaction;

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
import com.codefollower.lealone.omid.transaction.TransactionManager;
import com.codefollower.lealone.omid.transaction.Transaction;

public class TransactionManagerTest {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        TransactionManager tm = new TransactionManager(conf);
        Transaction t = tm.begin();

        Put put = new Put(Bytes.toBytes("2002"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("2002"));
        put(t, put);

        //tm.tryCommit(ts);
        //System.out.println(ts.tsoclient.validRead(ts.getCommitTimestamp(), ts.getStartTimestamp()));
        //System.out.println(ts.tsoclient.validRead(8, ts.getStartTimestamp()));

        TransactionManager.tsoclient.abort(t.getStartTimestamp());
        System.out.println(TransactionManager.tsoclient.validRead(t.getStartTimestamp() - 1, t.getStartTimestamp()));

        SyncAbortCompleteCallback c = new SyncAbortCompleteCallback();
        TransactionManager.tsoclient.completeAbort(t.getStartTimestamp(), c);
        c.await();

        for (int i = 0; i < 1000; i++) {
            //t = tm.begin();
            //put = new Put(Bytes.toBytes("2003"));
            put = new Put(Bytes.toBytes("" + (2003 + i)));
            put.add(Bytes.toBytes("f"), Bytes.toBytes("c"), Bytes.toBytes("2003"));
            put(t, put);
            //tm.commit(t);
        }

        tm.commit(t);

        //TransactionManager.close();
    }

    public static void put(Transaction t, Put put) throws IOException, IllegalArgumentException {
        final long startTimestamp = t.getStartTimestamp();
        // create put with correct ts
        final Put tsput = new Put(put.getRow(), startTimestamp); //把事务的开始时间戳放到Put里
        Map<byte[], List<KeyValue>> kvs = put.getFamilyMap();
        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                tsput.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), startTimestamp, kv.getValue()));
            }
        }

        // should add the table as well
        t.addRow(new RowKeyFamily(tsput.getRow(), Bytes.toBytes("mytable"), tsput.getFamilyMap()));
    }
}
