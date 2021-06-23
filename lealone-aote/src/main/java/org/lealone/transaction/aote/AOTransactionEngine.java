/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

import org.lealone.db.RunMode;
import org.lealone.storage.StorageMap;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.TransactionMap;

public class AOTransactionEngine extends AMTransactionEngine {

    private static final String NAME = "AOTE";

    public AOTransactionEngine() {
        super(NAME);
    }

    @Override
    protected AMTransaction createTransaction(long tid, RunMode runMode) {
        if (runMode == RunMode.REPLICATION || runMode == RunMode.SHARDING)
            return new AOTransaction(this, tid);
        else
            return new AMTransaction(this, tid);
    }

    @Override
    protected TransactionMap<?, ?> getTransactionMap(Transaction transaction,
            StorageMap<Object, TransactionalValue> map) {
        return new AOTransactionMap<>((AOTransaction) transaction, map);
    }

    @Override
    public boolean validateTransaction(String globalTransactionName) {
        return DTRValidator.validateTransaction(globalTransactionName);
    }

    static boolean validateTransaction(long tid, AOTransaction currentTransaction) {
        return DTRValidator.validateTransaction(tid, currentTransaction);
    }
}
