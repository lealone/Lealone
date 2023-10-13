/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.util.concurrent.LinkedTransferQueue;

import org.lealone.db.DataBufferFactory;

public interface TransactionHandler {

    int getHandlerId();

    void addTransaction(PendingTransaction pt);

    PendingTransaction getTransaction();

    void wakeUp();

    DataBufferFactory getDataBufferFactory();

    public default Object getNetEventLoop() {
        return null;
    }

    public static final DefaultTransactionHandler defaultTHandler = new DefaultTransactionHandler();

    public static class DefaultTransactionHandler implements TransactionHandler {

        // 在启动阶段，Scheduler还没启动，只有main线程在跑，但是redo log sync线程也会访问
        private final LinkedTransferQueue<PendingTransaction> pts = new LinkedTransferQueue<>();

        @Override
        public int getHandlerId() {
            return 0;
        }

        @Override
        public void addTransaction(PendingTransaction pt) {
            pts.add(pt);
        }

        @Override
        public PendingTransaction getTransaction() {
            return pts.poll();
        }

        @Override
        public void wakeUp() {
        }

        @Override
        public DataBufferFactory getDataBufferFactory() {
            return DataBufferFactory.getConcurrentFactory();
        }
    }
}
