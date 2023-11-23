/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.scheduler;

import org.lealone.db.session.Session;
import org.lealone.storage.page.PageOperationHandler;
import org.lealone.transaction.TransactionListener;

public class SchedulerThread extends Thread {

    private final Scheduler scheduler;

    public SchedulerThread(Scheduler scheduler) {
        super(scheduler);
        this.scheduler = scheduler;
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public PageOperationHandler getPageOperationHandler() {
        return scheduler;
    }

    public static PageOperationHandler currentPageOperationHandler() {
        Object t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler();
        } else {
            return null;
        }
    }

    public static TransactionListener currentTransactionListener() {
        Object t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler();
        } else {
            return null;
        }
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
        Object t = Thread.currentThread();
        if (t instanceof SchedulerThread) {
            return ((SchedulerThread) t).getScheduler();
        } else {
            return null;
        }
    }

    public static boolean isScheduler() {
        return Thread.currentThread() instanceof SchedulerThread;
    }
}
