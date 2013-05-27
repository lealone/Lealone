package com.codefollower.lealone.hbase.dbobject.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.lealone.omid.client.ColumnWrapper;
import com.codefollower.lealone.omid.transaction.Transaction;
import com.codefollower.lealone.omid.transaction.TransactionManager;

public class Filter {

    /** We always ask for CACHE_VERSIONS_OVERHEAD extra versions */
    private static final int CACHE_VERSIONS_OVERHEAD = 3;

    //改编自com.codefollower.lealone.omid.transaction.TTable.filter(Transaction, List<KeyValue>, int)
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
            if (TransactionManager.tsoclient.validRead(kv.getTimestamp(), startTimestamp)) {
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
}
