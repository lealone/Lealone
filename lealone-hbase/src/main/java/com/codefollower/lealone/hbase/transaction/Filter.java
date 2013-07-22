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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.lealone.hbase.metadata.TransactionStatusTable;
import com.codefollower.lealone.transaction.Transaction;

public class Filter {
    public final static Committed committed = new Committed();
    private final static Set<Long> aborted = Collections.synchronizedSet(new HashSet<Long>(1000));
    private static long largestDeletedTimestamp;
    private static long connectionTimestamp = 0;
    private static boolean hasConnectionTimestamp = false;

    /** We always ask for CACHE_VERSIONS_OVERHEAD extra versions */
    private static final int CACHE_VERSIONS_OVERHEAD = 3;

    public static List<KeyValue> filter(HRegionServer regionServer, byte[] regionName, Transaction transaction,
            List<KeyValue> kvs, int localVersions) throws IOException {
        if (kvs == null) {
            return Collections.emptyList();
        }

        final int requestVersions = localVersions * 2 + CACHE_VERSIONS_OVERHEAD;

        long startTimestamp = transaction.getStartTimestamp();
        // Filtered kvs
        List<KeyValue> filtered = new ArrayList<KeyValue>();
        // Map from column to older uncommitted timestamp
        List<Get> pendingGets = new ArrayList<Get>();
        ColumnWrapper lastColumn = new ColumnWrapper(null, null);
        long oldestUncommittedTS = Long.MAX_VALUE;
        boolean validRead = true;
        // Number of versions needed to reach a committed value
        int versionsProcessed = 0;

        for (KeyValue kv : kvs) {
            ColumnWrapper currentColumn = new ColumnWrapper(kv.getFamily(), kv.getQualifier());
            if (!currentColumn.equals(lastColumn)) {
                // New column, if we didn't read a committed value for last one,
                // add it to pending
                if (!validRead && versionsProcessed == localVersions) {
                    Get get = new Get(kv.getRow());
                    get.addColumn(kv.getFamily(), kv.getQualifier());
                    get.setMaxVersions(requestVersions); // TODO set maxVersions
                                                         // wisely
                    get.setTimeRange(0, oldestUncommittedTS - 1);
                    pendingGets.add(get);
                }
                validRead = false;
                versionsProcessed = 0;
                oldestUncommittedTS = Long.MAX_VALUE;
                lastColumn = currentColumn;
            }
            if (validRead) {
                // If we already have a committed value for this column, skip kv
                continue;
            }
            versionsProcessed++;
            if (validRead(regionServer.getServerName().getHostAndPort(), kv.getTimestamp(), startTimestamp)) {
                // Valid read, add it to result unless it's a delete
                if (kv.getValueLength() > 0) {
                    filtered.add(kv);
                }
                validRead = true;
            } else {
                // Uncomitted, keep track of oldest uncommitted timestamp
                oldestUncommittedTS = Math.min(oldestUncommittedTS, kv.getTimestamp());
            }
        }

        // If we have pending columns, request (and filter recursively) them
        if (!pendingGets.isEmpty()) {
            int size = pendingGets.size();
            Result[] results = new Result[size];
            for (int i = 0; i < size; i++)
                results[i] = regionServer.get(regionName, pendingGets.get(i));
            for (Result r : results) {
                filtered.addAll(filter(regionServer, regionName, transaction, r.list(), requestVersions));
            }
        }
        Collections.sort(filtered, KeyValue.COMPARATOR);
        return filtered;
    }

    public static boolean validRead(String hostAndPort, long queryTimestamp, long startTimestamp) throws IOException {
        if (queryTimestamp == startTimestamp)
            return true;
        if (queryTimestamp < startTimestamp & queryTimestamp % 2 == 0)
            return true;
        if (aborted.contains(queryTimestamp))
            return false;
        long commitTimestamp = committed.getCommit(queryTimestamp);

        if (commitTimestamp == -2)
            return false;
        else if (commitTimestamp != -1)
            return commitTimestamp <= startTimestamp;
        if (hasConnectionTimestamp && queryTimestamp > connectionTimestamp)
            return queryTimestamp <= largestDeletedTimestamp;
        if (queryTimestamp <= largestDeletedTimestamp)
            return true;

        commitTimestamp = TransactionStatusTable.getInstance().query(hostAndPort, queryTimestamp);
        if (commitTimestamp != -1) {
            committed.commit(queryTimestamp, commitTimestamp);
            return true;
        } else {
            committed.commit(queryTimestamp, -2);
            return false;
        }
    }
}
