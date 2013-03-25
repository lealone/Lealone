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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Provides the methods necessary to create and commit transactions.
 *
 * @see TransactionalTable
 *
 */
public class TransactionManager {
    private static final Log LOG = LogFactory.getLog(TransactionManager.class);
    private static TSOClient tsoclient = null;

    private final Configuration conf;
    private final HashMap<byte[], HTable> tableCache = new HashMap<byte[], HTable>();

    public TransactionManager(Configuration conf) throws TransactionException, IOException {
        this.conf = conf;
        if (tsoclient == null) {
            synchronized (TransactionManager.class) {
                if (tsoclient == null) {
                    tsoclient = new TSOClient(conf);
                }
            }
        }
    }

    /**
     * Starts a new transaction.
     *
     * This method returns an opaque {@link TransactionState} object, used by {@link TransactionalTable}'s methods
     * for performing operations on a given transaction.
     *
     * @return Opaque object which identifies one transaction.
     * @throws TransactionException
     */
    public TransactionState beginTransaction() throws TransactionException {
        SyncCreateCallback cb = new SyncCreateCallback();
        try {
            tsoclient.getNewTimestamp(cb);
            cb.await();
        } catch (Exception e) {
            throw new TransactionException("Could not get new timestamp", e);
        }
        if (cb.getException() != null) {
            throw new TransactionException("Error retrieving timestamp", cb.getException());
        }

        return new TransactionState(cb.getStartTimestamp(), tsoclient);
    }

    /**
     * Commits a transaction. If the transaction is aborted it automatically rollbacks the changes and
     * throws a {@link CommitUnsuccessfulException}.
     *
     * @param transactionState Object identifying the transaction to be committed.
     * @throws CommitUnsuccessfulException
     * @throws TransactionException
     */
    public void tryCommit(TransactionState transactionState) throws CommitUnsuccessfulException, TransactionException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("tryCommit " + transactionState.getStartTimestamp());
        }
        SyncCommitCallback cb = new SyncCommitCallback();
        try {
            tsoclient.commit(transactionState.getStartTimestamp(), transactionState.getRows(), cb);
            cb.await();
        } catch (Exception e) {
            throw new TransactionException("Could not commit", e);
        }
        if (cb.getException() != null) {
            throw new TransactionException("Error committing", cb.getException());
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("doneCommit " + transactionState.getStartTimestamp() + " TS_c: " + cb.getCommitTimestamp() + " Success: "
                    + (cb.getResult() == TSOClient.Result.OK));
        }

        if (cb.getResult() == TSOClient.Result.ABORTED) {
            cleanup(transactionState);
            throw new CommitUnsuccessfulException();
        }
        transactionState.setCommitTimestamp(cb.getCommitTimestamp());
    }

    /**
     * Aborts a transaction and automatically rollbacks the changes.
     *
     * @param transactionState Object identifying the transaction to be committed.
     * @throws TransactionException
     */
    public void abort(TransactionState transactionState) throws TransactionException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("abort " + transactionState.getStartTimestamp());
        }
        try {
            tsoclient.abort(transactionState.getStartTimestamp());
        } catch (Exception e) {
            throw new TransactionException("Could not abort", e);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("doneAbort " + transactionState.getStartTimestamp());
        }

        // Make sure its commit timestamp is 0, so the cleanup does the right job
        transactionState.setCommitTimestamp(0);
        cleanup(transactionState);
    }

    private void cleanup(final TransactionState transactionState) throws TransactionException {
        Map<byte[], List<Delete>> deleteBatches = new HashMap<byte[], List<Delete>>();
        for (final RowKeyFamily rowkey : transactionState.getRows()) {
            List<Delete> batch = deleteBatches.get(rowkey.getTable());
            if (batch == null) {
                batch = new ArrayList<Delete>();
                deleteBatches.put(rowkey.getTable(), batch);
            }
            Delete delete = new Delete(rowkey.getRow(), transactionState.getStartTimestamp(), null);
            //Delete delete = new Delete(rowkey.getRow());
            for (Entry<byte[], List<KeyValue>> entry : rowkey.getFamilies().entrySet()) {
                for (KeyValue kv : entry.getValue()) {
                    delete.deleteColumn(entry.getKey(), kv.getQualifier(), transactionState.getStartTimestamp());
                }
            }
            batch.add(delete);
        }
        for (final Entry<byte[], List<Delete>> entry : deleteBatches.entrySet()) {
            try {
                HTable table = tableCache.get(entry.getKey());
                if (table == null) {
                    table = new HTable(conf, entry.getKey());
                    tableCache.put(entry.getKey(), table);
                }
                table.delete(entry.getValue());
            } catch (IOException ioe) {
                throw new TransactionException("Could not clean up for table " + entry.getKey(), ioe);
            }
        }
        AbortCompleteCallback cb = new SyncAbortCompleteCallback();
        try {
            tsoclient.completeAbort(transactionState.getStartTimestamp(), cb);
        } catch (IOException ioe) {
            throw new TransactionException("Could not notify TSO about cleanup completion for transaction "
                    + transactionState.getStartTimestamp(), ioe);
        }
    }
}
