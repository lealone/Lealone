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

package com.codefollower.lealone.omid.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides transactional methods for accessing and modifying a given snapshot of data identified by an opaque
 * {@link TransactionState} object.
 *
 */
public class TransactionalTable extends HTable {
    /** We always ask for CACHE_VERSIONS_OVERHEAD extra versions */
    private static int CACHE_VERSIONS_OVERHEAD = 3;

    /** How fast do we adapt the average */
    private static final double alpha = 0.975;

    /** Average number of versions needed to reach the right snapshot */
    private double versionsAvg = 3;

    public TransactionalTable(Configuration conf, byte[] tableName) throws IOException {
        super(conf, tableName);
    }

    public TransactionalTable(Configuration conf, String tableName) throws IOException {
        this(conf, Bytes.toBytes(tableName));
    }

    /**
     * Transactional version of {@link HTable#get(Get)}
     * 
     * @param transactionState Identifier of the transaction
     * @see HTable#get(Get)
     * @throws IOException
     */
    public Result get(TransactionState transactionState, final Get get) throws IOException {
        final int requestedVersions = (int) (versionsAvg + CACHE_VERSIONS_OVERHEAD);
        final long readTimestamp = transactionState.getStartTimestamp();
        final Get tsget = new Get(get.getRow());
        TimeRange timeRange = get.getTimeRange();
        long startTime = timeRange.getMin();
        long endTime = Math.min(timeRange.getMax(), readTimestamp + 1);
        tsget.setTimeRange(startTime, endTime).setMaxVersions(requestedVersions);
        Map<byte[], NavigableSet<byte[]>> kvs = get.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null || qualifiers.isEmpty()) {
                tsget.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    tsget.addColumn(family, qualifier);
                }
            }
        }
        // Return the KVs that belong to the transaction snapshot, ask for more versions if needed
        return new Result(filter(transactionState, super.get(tsget).list(), requestedVersions));
    }

    /**
     * Transactional version of {@link HTable#delete(Delete)}
     * 
     * @param transactionState Identifier of the transaction
     * @see HTable#delete(Delete)
     * @throws IOException
     */
    public void delete(TransactionState transactionState, Delete delete) throws IOException {
        final long startTimestamp = transactionState.getStartTimestamp();
        boolean issueGet = false;

        final Put deleteP = new Put(delete.getRow(), startTimestamp);
        final Get deleteG = new Get(delete.getRow());
        Map<byte[], List<KeyValue>> fmap = delete.getFamilyMap();
        if (fmap.isEmpty()) {
            issueGet = true;
        }
        for (List<KeyValue> kvl : fmap.values()) {
            for (KeyValue kv : kvl) {
                switch (KeyValue.Type.codeToType(kv.getType())) {
                case DeleteColumn:
                    deleteP.add(kv.getFamily(), kv.getQualifier(), startTimestamp, null);
                    break;
                case DeleteFamily:
                    deleteG.addFamily(kv.getFamily());
                    issueGet = true;
                    break;
                case Delete:
                    if (kv.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
                        deleteP.add(kv.getFamily(), kv.getQualifier(), startTimestamp, null);
                        break;
                    } else {
                        throw new UnsupportedOperationException("Cannot delete specific versions on Snapshot Isolation.");
                    }
                }
            }
        }
        if (issueGet) {
            // It's better to perform a transactional get to avoid deleting more than necessary
            Result result = this.get(transactionState, deleteG);
            for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entryF : result.getMap().entrySet()) {
                byte[] family = entryF.getKey();
                for (Entry<byte[], NavigableMap<Long, byte[]>> entryQ : entryF.getValue().entrySet()) {
                    byte[] qualifier = entryQ.getKey();
                    deleteP.add(family, qualifier, null);
                }
            }
        }

        transactionState.addRow(new RowKeyFamily(delete.getRow(), getTableName(), deleteP.getFamilyMap()));

        put(deleteP);
    }

    /**
     * Transactional version of {@link HTable#put(Put)}
     * 
     * @param transactionState Identifier of the transaction
     * @see HTable#put(Put)
     * @throws IOException
     */
    public void put(TransactionState transactionState, Put put) throws IOException, IllegalArgumentException {
        final long startTimestamp = transactionState.getStartTimestamp();
        // create put with correct ts
        final Put tsput = new Put(put.getRow(), startTimestamp);
        Map<byte[], List<KeyValue>> kvs = put.getFamilyMap();
        for (List<KeyValue> kvl : kvs.values()) {
            for (KeyValue kv : kvl) {
                tsput.add(new KeyValue(kv.getRow(), kv.getFamily(), kv.getQualifier(), startTimestamp, kv.getValue()));
            }
        }

        // should add the table as well
        transactionState.addRow(new RowKeyFamily(tsput.getRow(), getTableName(), tsput.getFamilyMap()));

        put(tsput);
    }

    /**
     * Transactional version of {@link HTable#getScanner(Scan)}
     * 
     * @param transactionState Identifier of the transaction
     * @see HTable#getScanner(Scan)
     * @throws IOException
     */
    public ResultScanner getScanner(TransactionState transactionState, Scan scan) throws IOException {
        Scan tsscan = new Scan(scan);
        tsscan.setMaxVersions((int) (versionsAvg + CACHE_VERSIONS_OVERHEAD));
        tsscan.setTimeRange(0, transactionState.getStartTimestamp() + 1);
        return new ClientScanner(transactionState, getConfiguration(), tsscan, getTableName(),
                (int) (versionsAvg + CACHE_VERSIONS_OVERHEAD));
    }

    /**
     * Filters the raw results returned from HBase and returns only those belonging to the current snapshot, as
     * defined by the transactionState object. If the raw results don't contain enough information for a particular
     * qualifier, it will request more versions from HBase.
     *
     * @param transactionState Defines the current snapshot
     * @param kvs Raw KVs that we are going to filter
     * @param localVersions Number of versions requested from hbase
     * @return Filtered KVs belonging to the transaction snapshot
     * @throws IOException
     */
    private List<KeyValue> filter(TransactionState transactionState, List<KeyValue> kvs, int localVersions) throws IOException {
        final int requestVersions = localVersions * 2 + CACHE_VERSIONS_OVERHEAD;
        if (kvs == null) {
            return Collections.emptyList();
        }

        long startTimestamp = transactionState.getStartTimestamp();
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
                // New column, if we didn't read a committed value for last one, add it to pending
                if (!validRead && versionsProcessed == localVersions) {
                    Get get = new Get(kv.getRow());
                    get.addColumn(kv.getFamily(), kv.getQualifier());
                    get.setMaxVersions(requestVersions); // TODO set maxVersions wisely
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
            if (transactionState.tsoclient.validRead(kv.getTimestamp(), startTimestamp)) {
                // Valid read, add it to result unless it's a delete
                if (kv.getValueLength() > 0) {
                    filtered.add(kv);
                }
                validRead = true;
                // Update versionsAvg: increase it quickly, decrease it slowly
                versionsAvg = versionsProcessed > versionsAvg ? versionsProcessed : alpha * versionsAvg + (1 - alpha)
                        * versionsProcessed;
            } else {
                // Uncomitted, keep track of oldest uncommitted timestamp
                oldestUncommittedTS = Math.min(oldestUncommittedTS, kv.getTimestamp());
            }
        }

        // If we have pending columns, request (and filter recursively) them
        if (!pendingGets.isEmpty()) {
            Result[] results = this.get(pendingGets);
            for (Result r : results) {
                filtered.addAll(filter(transactionState, r.list(), requestVersions));
            }
        }
        Collections.sort(filtered, KeyValue.COMPARATOR);
        return filtered;
    }

    protected class ClientScanner extends org.apache.hadoop.hbase.client.ClientScanner {
        private TransactionState state;
        private int maxVersions;

        ClientScanner(TransactionState state, Configuration conf, Scan scan, byte[] tableName, int maxVersions)
                throws IOException {
            super(conf, scan, tableName);
            this.state = state;
            this.maxVersions = maxVersions;
        }

        @Override
        public Result next() throws IOException {
            List<KeyValue> filteredResult = Collections.emptyList();
            while (filteredResult.isEmpty()) {
                Result result = super.next();
                if (result == null) {
                    return null;
                }
                filteredResult = filter(state, result.list(), maxVersions);
            }
            return new Result(filteredResult);
        }

        // In principle no need to override, copied from super.next(int) to make sure it works even if super.next(int) 
        // changes its implementation
        @Override
        public Result[] next(int nbRows) throws IOException {
            // Collect values to be returned here
            ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
            for (int i = 0; i < nbRows; i++) {
                Result next = next();
                if (next != null) {
                    resultSets.add(next);
                } else {
                    break;
                }
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        }

    }

}
