/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.lock;

import org.lealone.db.lock.SimpleLockOwner;
import org.lealone.transaction.Transaction;

public class RowLockOwner extends SimpleLockOwner {

    private final Object oldValue;

    public RowLockOwner(Transaction transaction, Object oldValue) {
        super(transaction);
        this.oldValue = oldValue;
    }

    @Override
    public Object getOldValue() {
        return oldValue;
    }
}
