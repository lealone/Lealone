/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.scheduler;

import com.lealone.db.link.LinkableBase;

public abstract class SchedulerTaskManager extends LinkableBase<SchedulerTaskManager> {

    public abstract boolean gcCompletedTasks(InternalScheduler scheduler);

}
