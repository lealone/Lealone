/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tm;

import java.util.List;

import org.lealone.transaction.aote.AOTransaction;
import org.lealone.transaction.aote.AOTransactionEngine;

public abstract class TransactionManager {

    protected AOTransactionEngine te;
    protected int currentTransactionCount;

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

    public int currentTransactionCount() {
        return currentTransactionCount;
    }

    public static TransactionManager create(AOTransactionEngine te, boolean isSingleThread) {
        return isSingleThread ? new SingleThreadTransactionManager(te)
                : new ConcurrentTransactionManager(te);
    }
}
