/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.lock;

import org.lealone.transaction.Transaction;

// 单机模式，只保存transaction
public class SimpleLockOwner extends LockOwner {

    private final Transaction transaction;

    public SimpleLockOwner(Transaction transaction) {
        this.transaction = transaction;
    }

    @Override
    public Transaction getTransaction() {
        return transaction;
    }
}
