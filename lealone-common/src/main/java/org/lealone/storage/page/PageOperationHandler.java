/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

public interface PageOperationHandler {

    long getLoad();

    void handlePageOperation(PageOperation po);

    void addWaitingHandler(PageOperationHandler handler);

    void wakeUpWaitingHandlers();

    void wakeUp();

    class DummyPageOperationHandler implements PageOperationHandler {
        @Override
        public long getLoad() {
            return 0;
        }

        @Override
        public void handlePageOperation(PageOperation po) {
        }

        @Override
        public void addWaitingHandler(PageOperationHandler handler) {
        }

        @Override
        public void wakeUpWaitingHandlers() {
        }

        @Override
        public void wakeUp() {
        }
    }
}
