/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import org.lealone.db.scheduler.Scheduler;
import org.lealone.db.session.Session;

public interface PageOperation {

    public static enum PageOperationResult {
        SUCCEEDED,
        RETRY,
        LOCKED;
    }

    default PageOperationResult run(Scheduler scheduler) {
        return run(scheduler, true);
    }

    default PageOperationResult run(Scheduler scheduler, boolean waitingIfLocked) {
        return PageOperationResult.SUCCEEDED;
    }

    default Session getSession() {
        return null;
    }
}
