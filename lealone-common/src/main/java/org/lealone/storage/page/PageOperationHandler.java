/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

public interface PageOperationHandler {

    long getLoad();

    void handlePageOperation(PageOperation po);

    default void addWaitingHandler(PageOperationHandler handler) {
    }

    default void wakeUpWaitingHandlers() {
    }

    default void wakeUp() {
    }
}
