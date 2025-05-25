/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

public interface SchedulerTaskManager {

    public boolean gcCompletedTasks(InternalScheduler scheduler);

}
