/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

public interface PageOperationHandler {

    // 没有用getId，考虑到实现类可能继承自java.lang.Thread，它里面也有一个getId，会导致冲突
    int getHandlerId();

    long getLoad();

    void handlePageOperation(PageOperation po);

    void addWaitingHandler(PageOperationHandler handler);

    void wakeUpWaitingHandlers();

    void wakeUp();

    class DummyPageOperationHandler implements PageOperationHandler {
        @Override
        public int getHandlerId() {
            return -1;
        }

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
