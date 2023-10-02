/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.lock;

import org.lealone.db.lock.Lock;
import org.lealone.db.lock.LockOwner;
import org.lealone.db.session.Session;
import org.lealone.transaction.Transaction;
import org.lealone.transaction.aote.AOTransaction;

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
