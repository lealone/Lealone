/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

public interface AsyncTaskHandler extends AsyncHandler<AsyncTask> {

    @Override
    void handle(AsyncTask task);

    default void addPeriodicTask(AsyncPeriodicTask task) {
    }

    default void removePeriodicTask(AsyncPeriodicTask task) {
    }
}
