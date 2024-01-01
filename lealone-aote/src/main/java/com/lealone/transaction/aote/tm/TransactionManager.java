/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.tm;

import java.util.List;

import com.lealone.transaction.aote.AOTransaction;
import com.lealone.transaction.aote.AOTransactionEngine;

public abstract class TransactionManager {

    protected final AOTransactionEngine te;

    public TransactionManager(AOTransactionEngine te) {
        this.te = te;
    }

    protected void removeTransaction(AOTransaction t) {
        if (t != null && t.isRepeatableRead())
            te.decrementRrtCount();
    }

    public abstract AOTransaction removeTransaction(long tid, int bitIndex);

    public abstract void addTransaction(AOTransaction t);

    public abstract void currentTransactions(List<AOTransaction> list);

    public abstract int currentTransactionCount();

    public static TransactionManager create(AOTransactionEngine te, boolean isSingleThread) {
        return isSingleThread ? new SingleThreadTransactionManager(te)
                : new ConcurrentTransactionManager(te);
    }
}
