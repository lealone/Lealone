/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.lealone.db.session.Session;
import com.lealone.transaction.Transaction;

// 只有一个线程修改
public abstract class LockOwner {

    public Transaction getTransaction() {
        return null;
    }

    public Object getOldValue() {
        return null;
    }

    public boolean addWaitingSession(Session session, AtomicReference<LockOwner> ownerRef) {
        return false;
    }

    public AtomicReference<ConcurrentLinkedQueue<Session>> getWaitingSessionsRef() {
        return null;
    }
}
