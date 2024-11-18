/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.storage.page;

import com.lealone.db.scheduler.InternalScheduler;
import com.lealone.db.session.Session;

public interface PageOperation {

    public static enum PageOperationResult {
        SUCCEEDED,
        RETRY,
        LOCKED,
        FAILED;
    }

    default PageOperationResult run(InternalScheduler scheduler) {
        return run(scheduler, true);
    }

    default PageOperationResult run(InternalScheduler scheduler, boolean waitingIfLocked) {
        return PageOperationResult.SUCCEEDED;
    }

    default Session getSession() {
        return null;
    }
}
