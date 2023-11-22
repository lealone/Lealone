/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import org.lealone.db.link.Linkable;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.sql.PreparedSQLStatement.YieldableCommand;

public interface PendingTaskHandler extends Linkable<PendingTaskHandler> {

    Scheduler getScheduler();

    void setScheduler(Scheduler scheduler);

    PendingTask getPendingTask();

    void submitTask(PendingTask task);

    default void submitTask(AsyncTask task) {
        submitTask(new PendingTask(task));
    }

    void setYieldableCommand(YieldableCommand yieldableCommand);

    YieldableCommand getYieldableCommand();

    YieldableCommand getYieldableCommand(boolean checkTimeout, TimeoutListener timeoutListener);

    public static interface TimeoutListener {
        void onTimeout(YieldableCommand c, Throwable e);
    }
}
