/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tm;

import java.util.List;

import org.lealone.common.util.BitField;
import org.lealone.transaction.aote.AOTransaction;
import org.lealone.transaction.aote.AOTransactionEngine;

public class SingleThreadTransactionManager extends TransactionManager {

    private final BitField bf = new BitField(8);
    private AOTransaction[] transactions;
    private int currentTransactionCount;

    public SingleThreadTransactionManager(AOTransactionEngine te) {
        super(te);
        transactions = new AOTransaction[8];
    }

    @Override
    public AOTransaction removeTransaction(long tid, int bitIndex) {
        currentTransactionCount--;
        bf.clear(bitIndex);
        AOTransaction t = transactions[bitIndex];
        transactions[bitIndex] = null;
        super.removeTransaction(t);
        return t;
    }

    @Override
    public void addTransaction(AOTransaction t) {
        currentTransactionCount++;
        int index = bf.nextClearBit(0);
        bf.set(index);
        if (index >= transactions.length) {
            AOTransaction[] newTransactions = new AOTransaction[transactions.length * 2];
            System.arraycopy(transactions, 0, newTransactions, 0, transactions.length);
            transactions = newTransactions;
        }
        t.setBitIndex(index);
        transactions[index] = t;
    }

    @Override
    public void currentTransactions(List<AOTransaction> list) {
        AOTransaction[] transactions = this.transactions;
        for (int i = 0, length = transactions.length; i < length; i++) {
            AOTransaction t = transactions[i];
            if (t != null)
                list.add(t);
        }
    }

    @Override
    public int currentTransactionCount() {
        return currentTransactionCount;
    }
}
