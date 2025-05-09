/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class SchedulerLock {

    private static final AtomicReferenceFieldUpdater<SchedulerLock, InternalScheduler> //
    lockUpdater = AtomicReferenceFieldUpdater.newUpdater(SchedulerLock.class, //
            InternalScheduler.class, "lockOwner");
    private volatile InternalScheduler lockOwner;

    public boolean tryLock(InternalScheduler newLockOwner) {
        return tryLock(newLockOwner, true);
    }

    public boolean tryLock(InternalScheduler newLockOwner, boolean waitingIfLocked) {
        // 前面的操作被锁住了就算lockOwner相同后续的也不能再继续
        if (newLockOwner == lockOwner)
            return false;
        while (true) {
            if (lockUpdater.compareAndSet(this, null, newLockOwner))
                return true;
            InternalScheduler owner = lockOwner;
            if (waitingIfLocked && owner != null) {
                owner.addWaitingScheduler(newLockOwner);
            }
            // 解锁了，或者又被其他线程锁住了
            if (lockOwner == null || (waitingIfLocked && lockOwner != owner))
                continue;
            else
                return false;
        }
    }

    public void unlock() {
        if (lockOwner != null) {
            InternalScheduler owner = lockOwner;
            lockOwner = null;
            owner.wakeUpWaitingSchedulers();
        }
    }

    public boolean isLocked() {
        return lockOwner != null;
    }

    public InternalScheduler getLockOwner() {
        return lockOwner;
    }
}
