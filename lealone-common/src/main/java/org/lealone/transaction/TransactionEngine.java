/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.db.Constants;
import org.lealone.db.PluggableEngine;
import org.lealone.db.PluginManager;

public interface TransactionEngine extends PluggableEngine {

    public static TransactionEngine getDefaultTransactionEngine() {
        return PluginManager.getPlugin(TransactionEngine.class,
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
    }

    default Transaction beginTransaction(boolean autoCommit) {
        return beginTransaction(autoCommit, Transaction.IL_READ_COMMITTED);
    }

    Transaction beginTransaction(boolean autoCommit, int isolationLevel);

    boolean supportsMVCC();

    TransactionMap<?, ?> getTransactionMap(String mapName, Transaction transaction);

    void checkpoint();

    default Runnable getRunnable() {
        return null;
    }

    default boolean containsRepeatableReadTransactions() {
        return false;
    }

    default boolean containsTransaction(Long tid) {
        return false;
    }

    default Transaction getTransaction(Long tid) {
        return null;
    }

    default ConcurrentSkipListMap<Long, ? extends Transaction> currentTransactions() {
        return null;
    }

    default void fullGc(int schedulerCount, int schedulerId) {
    }

    default void addGcTask(GcTask gcTask) {
    }

    default void removeGcTask(GcTask gcTask) {
    }

    interface GcTask {
        void gc(TransactionEngine te);
    }
}
