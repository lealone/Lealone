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
import org.lealone.db.RunMode;

public interface TransactionEngine extends PluggableEngine {

    public static TransactionEngine getDefaultTransactionEngine() {
        return PluginManager.getPlugin(TransactionEngine.class,
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
    }

    default Transaction beginTransaction() {
        return beginTransaction(false);
    }

    default Transaction beginTransaction(boolean autoCommit) {
        return beginTransaction(autoCommit, RunMode.CLIENT_SERVER);
    }

    default Transaction beginTransaction(boolean autoCommit, RunMode runMode) {
        return beginTransaction(autoCommit, runMode, Transaction.IL_READ_COMMITTED);
    }

    default Transaction beginTransaction(int isolationLevel) {
        return beginTransaction(false, isolationLevel);
    }

    default Transaction beginTransaction(boolean autoCommit, int isolationLevel) {
        return beginTransaction(autoCommit, RunMode.CLIENT_SERVER, isolationLevel);
    }

    Transaction beginTransaction(boolean autoCommit, RunMode runMode, int isolationLevel);

    boolean supportsMVCC();

    TransactionMap<?, ?> getTransactionMap(String mapName, Transaction transaction);

    void checkpoint();

    default Runnable getFsyncService() {
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

    default void fullGc(int schedulerId) {
    }

    default void addGcTask(GcTask gcTask) {
    }

    default void removeGcTask(GcTask gcTask) {
    }

    interface GcTask {
        void gc(TransactionEngine te);
    }
}
