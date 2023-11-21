/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import org.lealone.db.link.LinkableBase;

public class PendingTask extends LinkableBase<PendingTask> {

    private final AsyncTask task;
    private boolean completed;

    public PendingTask(AsyncTask task) {
        this.task = task;
    }

    public AsyncTask getTask() {
        return task;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }
}
