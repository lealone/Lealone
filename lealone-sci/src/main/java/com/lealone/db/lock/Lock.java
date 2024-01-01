/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import java.util.concurrent.atomic.AtomicReference;

import com.lealone.db.session.Session;
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

    public boolean isLockedExclusivelyBy(Session session) {
        return ref.get().getTransaction() == session.getTransaction();
    }

    protected LockOwner createLockOwner(Transaction transaction, Object oldValue) {
        return new SimpleLockOwner(transaction);
    }

    // 子类可以用自己的方式增加锁
    protected void addLock(Session session, Transaction t) {
        session.addLock(this);
    }

    public boolean tryLock(Transaction t, Object key, Object oldValue) {
        Session session = t.getSession();
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
            if (old == t || old == t.getParentTransaction())
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

    public int addWaitingTransaction(Object key, Transaction lockedByTransaction, Session session) {
        if (lockedByTransaction == null)
            return Transaction.OPERATION_NEED_RETRY;

        return lockedByTransaction.addWaitingTransaction(key, session, null);

    }

    // 允许子类可以覆盖此方法
    public void unlock(Session oldSession, boolean succeeded, Session newSession) {
        unlock(oldSession, newSession);
    }

    public void unlock(Session oldSession, Session newSession) {
        ref.set(NULL);
    }
}
