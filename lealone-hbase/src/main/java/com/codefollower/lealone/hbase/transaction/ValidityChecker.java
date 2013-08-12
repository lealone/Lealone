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
package com.codefollower.lealone.hbase.transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.hbase.engine.HBaseConstants;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;

/**
 * 
 * 事务有效性检查
 *
 */
public class ValidityChecker {
    private final static TransactionStatusCache cache = new TransactionStatusCache();

    public static Result checkResult(byte[] defaultColumnFamilyName, HBaseSession session, HRegionServer regionServer,
            String hostAndPort, byte[] regionName, Transaction t, Result r) throws IOException {
        if (r == null || r.isEmpty())
            return null;
        byte[] bytes;
        long oldTid;
        long newTid = t.getTransactionId();

        bytes = r.getValue(defaultColumnFamilyName, HBaseConstants.TID);
        //遗留系统的HBase表不会有TID列，这类表的记录都认为是有效的
        if (bytes == null) {
            return r;
        }

        oldTid = Bytes.toLong(bytes);

        //记录已删除，不需要再处理
        bytes = r.getValue(defaultColumnFamilyName, HBaseConstants.TAG);
        if (bytes != null && Bytes.toShort(bytes) == HBaseConstants.Tag.DELETE) {
            return null;
        }

        //1. oldTid与newTid相等时，说明当前事务在读取它刚写入的记录
        //2. oldTid是偶数时，说明是非事务，如果oldTid小于newTid，那么就认为此条记录是有效的
        if (oldTid == newTid //
                || (oldTid % 2 == 0) && (oldTid < newTid)) {
            return r;
        }

        if (isValid(hostAndPort, oldTid, newTid, t)) {
            return r;
        } else {
            Get get = new Get(r.getRow());
            get.setMaxVersions(1);
            get.setTimeRange(0, oldTid - 1);
            r = regionServer.get(regionName, get);
            return checkResult(defaultColumnFamilyName, session, regionServer, hostAndPort, regionName, t, r);
        }
    }

    /**
     * 
     * @param hostAndPort 所要检查的行所在的主机名和端口号
     * @param oldTid 所要检查的行存入数据库的时间戳(旧事务id)
     * @param newTid 当前事务的开始时间戳(当前事务id)
     * @return
     * @throws IOException
     */
    private static boolean isValid(String hostAndPort, long oldTid, long newTid, Transaction transaction) throws IOException {
        long commitTimestamp = cache.get(oldTid); //TransactionStatusCache中的所有值初始情况下是-1

        //3. 上一次已经查过了，已确认过是条无效的记录
        if (commitTimestamp == -2)
            return false;

        //4. 是有效的事务记录，再进一步判断是否小于等于当前事务的开始时间戳
        if (commitTimestamp != -1)
            return commitTimestamp <= newTid;

        //5. 记录还没在TransactionStatusCache中，需要到TransactionStatusTable中查询(这一步会消耗一些时间)
        commitTimestamp = TransactionStatusTable.getInstance().query(hostAndPort, oldTid);
        if (commitTimestamp != -1) {
            cache.set(oldTid, commitTimestamp);
            return true;
        } else {
            cache.set(oldTid, -2);
            return false;
        }
    }

    public static Result[] fetchResults(byte[] defaultColumnFamilyName, HBaseSession session, String hostAndPort, //
            byte[] regionName, long scannerId, int fetchSize) throws IOException {
        Transaction t = session.getTransaction();
        Result r;
        Result[] result = session.getRegionServer().next(scannerId, fetchSize);
        ArrayList<Result> list = new ArrayList<Result>(result.length);
        for (int i = 0; i < result.length; i++) {
            r = checkResult(defaultColumnFamilyName, session, session.getRegionServer(), hostAndPort, regionName, t, result[i]);
            if (r != null)
                list.add(r);
        }

        return list.toArray(new Result[list.size()]);
    }

    public static boolean fetchResults(byte[] defaultColumnFamilyName, HBaseSession session, String hostAndPort, //
            byte[] regionName, InternalScanner scanner, int fetchSize, ArrayList<Result> list) throws IOException {
        Transaction t = session.getTransaction();
        Result r;
        List<KeyValue> kvs = new ArrayList<KeyValue>();

        //long start = System.nanoTime();
        boolean hasMoreRows = true;
        for (int i = 0; hasMoreRows && i < fetchSize; i++) {
            hasMoreRows = scanner.next(kvs);
            if (!kvs.isEmpty()) {
                r = checkResult(defaultColumnFamilyName, session, session.getRegionServer(), hostAndPort, regionName, t,
                        new Result(kvs));
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
