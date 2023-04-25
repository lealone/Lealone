/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.lock;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.lealone.db.DbObjectType;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;
import org.lealone.transaction.Transaction;

//数据库对象模型已经支持多版本，所以对象锁只需要像行锁一样实现即可
public class DbObjectLockImpl implements DbObjectLock {

    private final AtomicReference<Transaction> ref = new AtomicReference<>();
    private final DbObjectType type;
    private ArrayList<AsyncHandler<AsyncResult<Boolean>>> handlers;

    public DbObjectLockImpl(DbObjectType type) {
        this.type = type;
    }

    @Override
    public DbObjectType getDbObjectType() {
        return type;
    }

    @Override
    public void addHandler(AsyncHandler<AsyncResult<Boolean>> handler) {
        if (handlers == null)
            handlers = new ArrayList<>(1);
        handlers.add(handler);
    }

    @Override
    public boolean lock(ServerSession session, boolean exclusive) {
        if (exclusive)
            return tryExclusiveLock(session);
        else
            return trySharedLock(session);
    }

    @Override
    public boolean trySharedLock(ServerSession session) {
        return true;
    }

    @Override
    public boolean tryExclusiveLock(ServerSession session) {
        while (true) {
            if (ref.get() == session.getTransaction())
                return true;
            if (ref.compareAndSet(null, session.getTransaction())) {
                session.addLock(this);
                return true;
            } else {
                if (addWaitingTransaction(ref.get(), session) == Transaction.OPERATION_NEED_WAIT)
                    return false;
            }
        }
    }

    private int addWaitingTransaction(Transaction oldTransaction, ServerSession session) {
        if (oldTransaction != null)
            return oldTransaction.addWaitingTransaction(this, session.getTransaction(),
                    session.getTransactionListener());
        else
            return Transaction.OPERATION_NEED_RETRY;
    }

    @Override
    public void unlock(ServerSession session, boolean succeeded) {
        if (ref.compareAndSet(session.getTransaction(), null)) {
            if (handlers != null) {
                handlers.forEach(h -> {
                    h.handle(new AsyncResult<>(succeeded));
                });
                handlers = null;
            }
        }
    }

    @Override
    public boolean isLockedExclusivelyBy(ServerSession session) {
        return ref.get() == session.getTransaction();
    }
}
