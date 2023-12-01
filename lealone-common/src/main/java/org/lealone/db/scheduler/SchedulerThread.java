/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import org.lealone.db.session.Session;

public class SchedulerThread extends Thread {

    private final static ThreadLocal<Scheduler> threadLocal = new ThreadLocal<>();

    private final Scheduler scheduler;

    public SchedulerThread(Scheduler scheduler) {
        super(scheduler);
        this.scheduler = scheduler;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public static Session currentSession() {
        Object t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler().getCurrentSession();
        } else {
            return null;
        }
    }

    public static Object currentObject() {
        Object t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler();
        } else {
            return t;
        }
    }

    public static Scheduler currentScheduler() {
        Thread t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler();
        } else {
            return null;
        }
    }

    public static Scheduler currentScheduler(SchedulerFactory sf) {
        Thread t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler();
        } else {
            Scheduler scheduler = threadLocal.get();
            if (scheduler == null) {
                scheduler = sf.bindScheduler(t);
                threadLocal.set(scheduler);
            }
            return scheduler;
        }
    }

    public static void bindScheduler(Scheduler scheduler) {
        if (isScheduler())
            return;
        if (threadLocal.get() != scheduler)
            threadLocal.set(scheduler);
    }

    public static boolean isScheduler() {
        return Thread.currentThread() instanceof SchedulerThread;
    }
}
