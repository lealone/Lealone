/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import java.util.concurrent.atomic.AtomicReference;

import com.lealone.db.session.InternalSession;
import com.lealone.storage.page.PageListener;
import com.lealone.transaction.Transaction;

public abstract class Lock {

    static final NullLockOwner NULL = new NullLockOwner();

    private final AtomicReference<LockOwner> ref = new AtomicReference<>(NULL);

    public abstract String getLockType();

    public Transaction getTransaction() {
        return ref.get().getTransaction();
    }

    public Object getOldValue() {
        return ref.get().getOldValue();
    }

    public boolean isLockedExclusivelyBy(InternalSession session) {
        return ref.get().getTransaction() == session.getTransaction();
    }

    protected LockOwner createLockOwner(Transaction transaction, Object oldValue) {
        return new SimpleLockOwner(transaction);
    }

    // 子类可以用自己的方式增加锁
    protected void addLock(InternalSession session, Transaction t) {
        session.addLock(this);
    }

    public boolean tryLock(Transaction t, Object key, Object oldValue) {
        InternalSession session = t.getSession();
        while (true) {
            // 首次调用tryLock时为null
            if (ref.get() == NULL) {
                LockOwner owner = createLockOwner(t, oldValue);
                if (ref.compareAndSet(NULL, owner)) {
                    addLock(session, t);
                    return true;
                }
            }

            // 避免重复加锁
            Transaction old = ref.get().getTransaction();
            // 只有old不为null时才需要判断
            if (old != null && (old == t || old == t.getParentTransaction()))
                return true;

            // 被其他事务占用时需要等待
            if (ref.get().getTransaction() != null) {
                if (addWaitingTransaction(key, ref.get().getTransaction(),
                        session) == Transaction.OPERATION_NEED_WAIT) {
                    return false;
                }
            }
        }
    }

    public int addWaitingTransaction(Object lockedObject, Transaction lockedByTransaction,
            InternalSession session) {
        if (lockedByTransaction == null)
            return Transaction.OPERATION_NEED_RETRY;
        return lockedByTransaction.addWaitingTransaction(lockedObject, session, null);
    }

    // 允许子类可以覆盖此方法
    public void unlock(InternalSession oldSession, boolean succeeded, InternalSession newSession) {
        unlock(oldSession, newSession);
    }

    public void unlock(InternalSession oldSession, InternalSession newSession) {
        ref.set(NULL);
    }

    public void unlockFast() {
        ref.set(NULL);
    }

    public LockOwner getLockOwner() {
        return ref.get();
    }

    public Lockable getLockable() {
        return null;
    }

    public void setLockable(Lockable lockable) {
    }

    public PageListener getPageListener() {
        return null;
    }

    public void setPageListener(PageListener pageListener) {
    }

    public boolean isPageLock() {
        return false;
    }

    @SuppressWarnings("unchecked")
    public static <V> V getLockedValue(Lockable lockable) {
        Object v = lockable.getLockedValue();
        if (v == null) {
            Lock lock = lockable.getLock();
            if (lock != null && lock.getOldValue() != null)
                v = lock.getOldValue();
        }
        return (V) v;
    }
}
