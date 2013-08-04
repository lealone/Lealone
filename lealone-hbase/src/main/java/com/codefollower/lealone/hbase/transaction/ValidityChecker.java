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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;
import com.codefollower.lealone.transaction.Transaction;

/**
 * 
 * 事务有效性检查
 *
 */
public class ValidityChecker {
    public final static TransactionStatusCache cache = new TransactionStatusCache();

    /** We always ask for CACHE_VERSIONS_OVERHEAD extra versions */
    private static final int CACHE_VERSIONS_OVERHEAD = 3;

    public static List<KeyValue> check(HRegionServer regionServer, String hostAndPort, byte[] regionName,
            Transaction transaction, List<KeyValue> kvs, int localVersions) throws IOException {
        if (kvs == null) {
            return Collections.emptyList();
        }

        final int requestVersions = localVersions * 2 + CACHE_VERSIONS_OVERHEAD;

        long startTimestamp = transaction.getStartTimestamp();
        List<KeyValue> checked = new ArrayList<KeyValue>();
        // Map from column to older uncommitted timestamp
        List<Get> pendingGets = new ArrayList<Get>();

        byte[] lastFamily = null;
        byte[] lastQualifier = null;

        byte[] currentFamily = null;
        byte[] currentQualifier = null;

        long oldestUncommittedTS = Long.MAX_VALUE;
        boolean isValidRead = true;
        // Number of versions needed to reach a committed value
        int versionsProcessed = 0;

        for (KeyValue kv : kvs) {
            currentFamily = kv.getFamily();
            currentQualifier = kv.getQualifier();

            //先比较qualifier再比较family的性能比反过来好，因为通常family都相等，只是qualifier不相等，
            //这样只要确认qualifier不相等就知道是一个新列了，不需要再多余比较family
            if (!(Bytes.equals(lastQualifier, currentQualifier) && Bytes.equals(lastFamily, currentFamily))) {
                // New column, if we didn't read a committed value for last one,
                // add it to pending
                if (!isValidRead && versionsProcessed == localVersions) {
                    Get get = new Get(kv.getRow());
                    get.addColumn(kv.getFamily(), kv.getQualifier());
                    get.setMaxVersions(requestVersions); // TODO set maxVersions wisely
                    get.setTimeRange(0, oldestUncommittedTS - 1);
                    pendingGets.add(get);
                }
                isValidRead = false;
                versionsProcessed = 0;
                oldestUncommittedTS = Long.MAX_VALUE;
                lastFamily = currentFamily;
                lastQualifier = currentQualifier;
            }
            if (isValidRead) {
                // If we already have a committed value for this column, skip kv
                continue;
            }
            versionsProcessed++;
            if (isValidRead(hostAndPort, kv.getTimestamp(), startTimestamp)) {
                // Valid read, add it to result unless it's a delete
                if (kv.getValueLength() > 0) {
                    checked.add(kv);
                }
                isValidRead = true;
            } else {
                // Uncomitted, keep track of oldest uncommitted timestamp
                oldestUncommittedTS = Math.min(oldestUncommittedTS, kv.getTimestamp());
            }
        }

        // If we have pending columns, request (and check recursively) them
        if (!pendingGets.isEmpty()) {
            int size = pendingGets.size();
            Result[] results = new Result[size];
            for (int i = 0; i < size; i++)
                results[i] = regionServer.get(regionName, pendingGets.get(i));
            for (Result r : results) {
                checked.addAll(check(regionServer, hostAndPort, regionName, transaction, r.list(), requestVersions));
            }
        }
        Collections.sort(checked, KeyValue.COMPARATOR);
        return checked;
    }

    /**
     * 
     * @param hostAndPort 所要检查的行所在的主机名和端口号
     * @param queryTimestamp 所要检查的行存入数据库的时间戳
     * @param startTimestamp 当前事务的开始时间戳
     * @return
     * @throws IOException
     */
    private static boolean isValidRead(String hostAndPort, long queryTimestamp, long startTimestamp) throws IOException {
        //1. 入库时间戳等于当前事务的开始时间戳，说明当前事务在读取它刚写入的记录
        if (queryTimestamp == startTimestamp)
            return true;

        //2. 时间戳是偶数时，说明是非事务，如果入库时间戳小于当前事务的开始时间戳，那么就认为此条记录是有效的
        if (queryTimestamp % 2 == 0)
            return queryTimestamp < startTimestamp;

        long commitTimestamp = cache.get(queryTimestamp); //TransactionStatusCache中的所有值初始情况下是-1

        //3. 上一次已经查过了，已确认过是条无效的记录
        if (commitTimestamp == -2)
            return false;

        //4. 是有效的事务记录，再进一步判断是否小于等于当前事务的开始时间戳
        if (commitTimestamp != -1)
            return commitTimestamp <= startTimestamp;

        //5. 记录还没在TransactionStatusCache中，需要到TransactionStatusTable中查询(这一步会消耗一些时间)
        commitTimestamp = TransactionStatusTable.getInstance().query(hostAndPort, queryTimestamp);
        if (commitTimestamp != -1) {
            cache.set(queryTimestamp, commitTimestamp);
            return true;
        } else {
            cache.set(queryTimestamp, -2);
            return false;
        }
    }
}
