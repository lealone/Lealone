/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction.aote.lock;

import com.lealone.db.lock.Lock;
import com.lealone.db.lock.LockOwner;
import com.lealone.db.lock.Lockable;
import com.lealone.db.session.InternalSession;
import com.lealone.storage.page.PageListener;
import com.lealone.transaction.Transaction;
import com.lealone.transaction.aote.AOTransaction;

public class RowLock extends Lock {

    private final Lockable lockable;

    public RowLock(Lockable lockable) {
        this.lockable = lockable;
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
        InternalSession session = t.getSession();
        // 单元测试时session为null
        if (session != null)
            session.removeLock(this);
        else
            t.removeLock(this);
    }

    @Override
    protected void addLock(InternalSession session, Transaction t) {
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
    public void unlock(InternalSession oldSession, InternalSession newSession) {
        // 先置null再unlock，否则其他事务加锁成功后又被当前事务置null，会导致两个新事务都加锁成功
        PageListener pListener = getPageListener();
        if (pListener != null)
            lockable.setLock(pListener.getLock());
        else
            lockable.setLock(null); // 重置为NULL，释放RowLock对象占用的内存
        super.unlock(oldSession, newSession);
    }

    @Override
    public Lockable getLockable() {
        return lockable;
    }

    private volatile PageListener pageListener;

    @Override
    public PageListener getPageListener() {
        return pageListener;
    }

    @Override
    public void setPageListener(PageListener pageListener) {
        this.pageListener = pageListener;
    }
}
