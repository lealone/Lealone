/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import org.lealone.db.session.Session;

public interface PageOperationHandler {

    // 没有用getId，考虑到实现类可能继承自java.lang.Thread，它里面也有一个getId，会导致冲突
    int getHandlerId();

    long getLoad();

    void handlePageOperation(PageOperation po);

    void addWaitingHandler(PageOperationHandler handler);

    void wakeUpWaitingHandlers();

    void wakeUp();

    default Session getCurrentSession() {
        return null;
    }

    default void setCurrentSession(Session currentSession) {
    }

    default boolean isScheduler() {
        return false;
    }
}
