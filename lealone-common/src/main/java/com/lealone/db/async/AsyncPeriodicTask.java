/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

import com.lealone.db.link.LinkableBase;

public class AsyncPeriodicTask extends LinkableBase<AsyncPeriodicTask> implements AsyncTask {

    private final long delay;
    private Runnable runnable;
    private long last;
    private boolean canceled;

    public AsyncPeriodicTask(long delay, Runnable runnable) {
        this(delay, delay, runnable);
    }

    public AsyncPeriodicTask(long initialDelay, long delay, Runnable runnable) {
        this.delay = delay;
        this.runnable = runnable;

        if (initialDelay > 0) {
            last = System.currentTimeMillis() + initialDelay;
        } else {
            last = System.currentTimeMillis() + delay;
        }
    }

    @Override
    public boolean isPeriodic() {
        return true;
    }

    @Override
    public int getPriority() {
        return MIN_PRIORITY;
    }

    public void cancel() {
        canceled = true;
    }

    public boolean isCancelled() {
        return canceled;
    }

    @Override
    public void run() {
        if (canceled)
            return;
        long now = System.currentTimeMillis();
        if (now > last) {
            last = now + delay;
            runnable.run();
        }
    }

    public long getDelay() {
        return delay;
    }

    public void setRunnable(Runnable runnable) {
        this.runnable = runnable;
    }

    public void resetLast() {
        last = 0;
    }
}
