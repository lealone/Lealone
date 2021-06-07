/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

public interface AsyncPeriodicTask extends AsyncTask {

    @Override
    default boolean isPeriodic() {
        return true;
    }

    @Override
    default int getPriority() {
        return MIN_PRIORITY;
    }
}
