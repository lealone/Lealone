package com.codefollower.lealone.hbase.transaction;

import java.util.HashSet;
import java.util.Set;

/**
 * 
 * This class contains the required information to represent an Omid's
 * transaction, including the set of rows modified.
 * 
 */
public class Transaction {
    private final Set<RowKeyFamily> rows;
    private final long startTimestamp;
    private long commitTimestamp;
    private boolean rollbackOnly;

    Transaction(long startTimestamp) {
        this.rows = new HashSet<RowKeyFamily>();
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = 0;
    }

    public long getTransactionId() {
        return startTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    public RowKeyFamily[] getRows() {
        return rows.toArray(new RowKeyFamily[0]);
    }

    public void addRow(RowKeyFamily row) {
        rows.add(row);
    }

    public String toString() {
        return "Transaction-" + Long.toHexString(startTimestamp);
    }

    /**
     * Modify the transaction associated with the current thread such that the
     * only possible outcome of the transaction is to roll back the transaction.
     */
    public void setRollbackOnly() {
        rollbackOnly = true;
    }

    public boolean isRollbackOnly() {
        return rollbackOnly;
    }
}
