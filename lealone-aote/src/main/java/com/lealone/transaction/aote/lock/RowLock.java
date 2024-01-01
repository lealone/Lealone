/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.lock;

import com.lealone.db.lock.Lock;
import com.lealone.db.lock.LockOwner;
import com.lealone.db.session.Session;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.aote.AOTransaction;

public class RowLock extends Lock {

    @Override
    public String getLockType() {
        return "RowLock";
    }

    @Override
    public AOTransaction getTransaction() {
        return (AOTransaction) super.getTransaction();
    }

    @Override
    protected void addLock(Session session, Transaction t) {
        // 单元测试时session为null
        if (session != null)
            session.addLock(this);
        else
            ((AOTransaction) t).addLock(this);
    }

    @Override
    protected LockOwner createLockOwner(Transaction transaction, Object oldValue) {
        return new RowLockOwner(transaction, oldValue);
    }

    public boolean isInsert() {
        return false;
    }
}
