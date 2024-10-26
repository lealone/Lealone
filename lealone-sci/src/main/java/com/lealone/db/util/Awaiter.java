/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.lealone.common.logging.Logger;

public class Awaiter {

    private final Logger logger;
    private final Semaphore semaphore = new Semaphore(1);
    private final AtomicBoolean waiting = new AtomicBoolean(false);
    private volatile boolean haveWork;

    public Awaiter(Logger logger) {
        this.logger = logger;
    }

    public void doAwait(long timeout) {
        if (waiting.compareAndSet(false, true)) {
            if (haveWork) {
                haveWork = false;
            } else {
                try {
                    semaphore.tryAcquire(timeout, TimeUnit.MILLISECONDS);
                    semaphore.drainPermits();
                } catch (Exception e) {
                    logger.warn("Semaphore tryAcquire exception", e);
                }
            }
            waiting.set(false);
        }
    }

    public void wakeUp() {
        haveWork = true;
        if (waiting.compareAndSet(true, false)) {
            semaphore.release(1);
        }
    }

    public void wakeUp(boolean force) {
        haveWork = true;
        if (force) {
            waiting.set(false);
            semaphore.release(1);
        } else {
            if (waiting.compareAndSet(true, false)) {
                semaphore.release(1);
            }
        }
    }
}
