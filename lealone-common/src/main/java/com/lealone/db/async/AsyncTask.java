/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.async;

public interface AsyncTask extends Runnable {

    public final static int MIN_PRIORITY = 1;
    public final static int NORM_PRIORITY = 5;
    public final static int MAX_PRIORITY = 10;

    default int getPriority() {
        return NORM_PRIORITY;
    }

    default boolean isPeriodic() {
        return false;
    }
}
