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
import com.lealone.transaction.aote.TransactionalValue;

public class RowLock extends Lock {

    private final TransactionalValue tv;

    public RowLock(TransactionalValue tv) {
        this.tv = tv;
    }

    @Override
    public String getLockType() {
        return "RowLock";
    }

    @Override
    public AOTransaction getTransaction() {
        return (AOTransaction) super.getTransaction();
    }

    public void removeLock(AOTransaction t) {
        Session session = t.getSession();
        // 单元测试时session为null
        if (session != null)
            session.removeLock(this);
        else
            t.removeLock(this);
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

    @Override
    public void unlock(Session oldSession, Session newSession) {
        // 先reset再unlock，否则其他事务加锁成功后又被当前事务rest到新的，会导致两个新事务都加锁成功
        tv.resetRowLock(); // 重置为NULL，释放RowLock对象占用的内存
        super.unlock(oldSession, newSession);
    }
}
