/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import com.lealone.storage.page.PageListener;

public abstract class LockableBase implements Lockable {

    private static final AtomicReferenceFieldUpdater<LockableBase, Lock> lockUpdater = //
            AtomicReferenceFieldUpdater.newUpdater(LockableBase.class, Lock.class, "lock");

    private volatile Lock lock;

    @Override
    public Lock getLock() {
        return lock;
    }

    @Override
    public void setLock(Lock lock) {
        this.lock = lock;
    }

    @Override
    public boolean compareAndSetLock(Lock expect, Lock update) {
        return lockUpdater.compareAndSet(this, expect, update);
    }

    @Override
    public PageListener getPageListener() {
        return getLock().getPageListener();
    }

    @Override
    public boolean isNoneLock() {
        Lock lock = this.lock;
        return lock == null || lock.isPageLock();
    }
}
