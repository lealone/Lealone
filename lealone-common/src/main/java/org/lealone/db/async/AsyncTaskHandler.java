/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface AsyncTaskHandler extends AsyncHandler<AsyncTask> {

    @Override
    void handle(AsyncTask task);

    ScheduledFuture<?> scheduleWithFixedDelay(AsyncTask task, long initialDelay, long delay, TimeUnit unit);

    default void addPeriodicTask(AsyncPeriodicTask task) {
    }

    default void removePeriodicTask(AsyncPeriodicTask task) {
    }
}
