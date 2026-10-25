/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.lock;

import com.lealone.db.lock.SimpleLockOwner;
import com.lealone.transaction.Transaction;

public class RowLockOwner extends SimpleLockOwner {

    private volatile Object oldValue;

    public RowLockOwner(Transaction transaction, Object oldValue) {
        super(transaction);
        this.oldValue = oldValue;
    }

    @Override
    public Object getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(Object oldValue) {
        this.oldValue = oldValue;
    }
}
