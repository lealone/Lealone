/*
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
package org.lealone.hbase.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.lealone.hbase.engine.HBaseConstants;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.metadata.TransactionStatusTable;
import org.lealone.util.StringUtils;

/**
 * 
 * 事务有效性检查
 *
 */
public class ValidityChecker {
    private final static TransactionStatusTable transactionStatusTable = TransactionStatusTable.getInstance();

    public static Result checkResult(byte[] defaultColumnFamilyName, HBaseSession session, HRegionServer regionServer,
            byte[] regionName, Transaction t, Result r) throws IOException {
        if (r == null || r.isEmpty())
            return null;
        byte[] bytes;
        long oldTid;
        long newTid = t.getTransactionId();

        bytes = r.getValue(defaultColumnFamilyName, HBaseConstants.TRANSACTION_META);
        //遗留系统的HBase表不会有TRANSACTION_META列，这类表的记录都认为是有效的
        if (bytes == null) {
            return r;
        }

        String[] transactionMeta = StringUtils.arraySplit(Bytes.toString(bytes), ',', false);
        String hostAndPort = transactionMeta[0];
        oldTid = Long.parseLong(transactionMeta[1]);
        if (Short.parseShort(transactionMeta[2]) == HBaseConstants.Tag.DELETE) {
            return null;
        }

        //1. oldTid与newTid相等时，说明当前事务在读取它刚写入的记录
        //2. oldTid是偶数时，说明是非事务，如果oldTid小于newTid，那么就认为此条记录是有效的
        if (oldTid == newTid //
                || (oldTid % 2 == 0) && (oldTid < newTid)) {
            return r;
        }

        if ((oldTid % 2 == 0) || !transactionStatusTable.isValid(hostAndPort, oldTid, t)) {
            Get get = new Get(r.getRow());
            get.setMaxVersions(1);
            get.setTimeRange(0, oldTid - 1);
            r = regionServer.get(regionName, get);
            return checkResult(defaultColumnFamilyName, session, regionServer, regionName, t, r);
        } else {
            return r;
        }
    }

    public static Result[] fetchResults(byte[] defaultColumnFamilyName, HBaseSession session, //
            byte[] regionName, long scannerId, int fetchSize) throws IOException {
        Transaction t = session.getTransaction();
        Result r;
        Result[] result = session.getRegionServer().next(scannerId, fetchSize);
        ArrayList<Result> list = new ArrayList<Result>(result.length);
        for (int i = 0; i < result.length; i++) {
            r = checkResult(defaultColumnFamilyName, session, session.getRegionServer(), regionName, t, result[i]);
            if (r != null)
                list.add(r);
        }

        return list.toArray(new Result[list.size()]);
    }

    public static boolean fetchResults(byte[] defaultColumnFamilyName, HBaseSession session, //
            byte[] regionName, InternalScanner scanner, int fetchSize, ArrayList<Result> list) throws IOException {
        Transaction t = session.getTransaction();
        Result r;
        List<KeyValue> kvs = new ArrayList<KeyValue>();

        //long start = System.nanoTime();
        boolean hasMoreRows = true;
        for (int i = 0; hasMoreRows && i < fetchSize; i++) {
            hasMoreRows = scanner.next(kvs);
            if (!kvs.isEmpty()) {
                r = checkResult(defaultColumnFamilyName, session, session.getRegionServer(), regionName, t, new Result(kvs));
                if (r != null)
                    list.add(r);
            }

            kvs.clear();
        }
        //long end = System.nanoTime();
        //System.out.println((end - start) / 1000000 + " ms count="+list.size());
        return hasMoreRows;
    }
}
