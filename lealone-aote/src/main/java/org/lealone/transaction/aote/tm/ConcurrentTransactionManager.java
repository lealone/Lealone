/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tm;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.transaction.aote.AOTransaction;
import org.lealone.transaction.aote.AOTransactionEngine;

public class ConcurrentTransactionManager extends TransactionManager {

    // key: transactionId
    private final ConcurrentHashMap<Long, AOTransaction> currentTransactions = new ConcurrentHashMap<>();

    public ConcurrentTransactionManager(AOTransactionEngine te) {
        super(te);
    }

    @Override
    public AOTransaction removeTransaction(long tid, int bitIndex) {
        currentTransactionCount--;
        AOTransaction t = currentTransactions.remove(tid);
        super.removeTransaction(t);
        return t;
    }

    @Override
    public void addTransaction(AOTransaction t) {
        currentTransactionCount++;
        currentTransactions.put(t.getTransactionId(), t);
    }

    @Override
    public void currentTransactions(List<AOTransaction> list) {
        list.addAll(currentTransactions.values());
    }
}
