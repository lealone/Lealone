/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.session.SessionStatus;

public class WaitingTransaction {

    private final Object key;
    private final Transaction transaction;
    private final TransactionListener listener;

    public WaitingTransaction(Object key, Transaction transaction, TransactionListener listener) {
        this.key = key;
        this.transaction = transaction;
        this.listener = listener;
    }

    public void wakeUp() {
        // 避免重复调用
        if (transaction.getSession().getStatus() == SessionStatus.WAITING) {
            transaction.getSession().setStatus(SessionStatus.RETRYING);
            if (listener != null)
                listener.wakeUp();
        }
    }

    public Object getKey() {
        return key;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public TransactionListener getListener() {
        return listener;
    }
}
