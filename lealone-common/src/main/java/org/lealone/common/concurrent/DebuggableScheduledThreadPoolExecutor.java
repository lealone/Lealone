/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.concurrent;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.lealone.common.util.JVMStabilityInspector;

/**
 * Like DebuggableThreadPoolExecutor, DebuggableScheduledThreadPoolExecutor always
 * logs exceptions from the tasks it is given, even if Future.get is never called elsewhere.
 *
 * DebuggableScheduledThreadPoolExecutor also catches exceptions during Task execution
 * so that they don't supress subsequent invocations of the task.
 */
public class DebuggableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor {
    public DebuggableScheduledThreadPoolExecutor(int corePoolSize, String threadPoolName, int priority) {
        super(corePoolSize, new NamedThreadFactory(threadPoolName, priority));
    }

    public DebuggableScheduledThreadPoolExecutor(String threadPoolName) {
        this(1, threadPoolName, Thread.NORM_PRIORITY);
    }

    // We need this as well as the wrapper for the benefit of non-repeating tasks
    @Override
    public void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        DebuggableThreadPoolExecutor.logExceptionsAfterExecute(r, t);
    }

    // override scheduling to supress exceptions that would cancel future executions
    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period,
            TimeUnit unit) {
        return super.scheduleAtFixedRate(new UncomplainingRunnable(command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay,
            TimeUnit unit) {
        return super.scheduleWithFixedDelay(new UncomplainingRunnable(command), initialDelay, delay,
                unit);
    }

    private static class UncomplainingRunnable implements Runnable {
        private final Runnable runnable;

        public UncomplainingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                JVMStabilityInspector.inspectThrowable(t);
                DebuggableThreadPoolExecutor.handleOrLog(t);
            }
        }
    }
}
