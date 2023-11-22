/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import org.lealone.db.scheduler.Scheduler;

public interface PendingTaskHandler {

    Scheduler getScheduler();

    void setScheduler(Scheduler scheduler);

    PendingTask getPendingTask();

    void submitTask(PendingTask task);

    default void submitTask(AsyncTask task) {
        submitTask(new PendingTask(task));
    }
}
