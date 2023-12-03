/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import java.util.List;

import org.lealone.db.Constants;
import org.lealone.db.PluggableEngine;
import org.lealone.db.PluginManager;
import org.lealone.db.RunMode;
import org.lealone.db.scheduler.Scheduler;

public interface TransactionEngine extends PluggableEngine {

    public static TransactionEngine getDefaultTransactionEngine() {
        return PluginManager.getPlugin(TransactionEngine.class,
                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
    }

    default Transaction beginTransaction() {
        return beginTransaction(RunMode.CLIENT_SERVER);
    }

    default Transaction beginTransaction(int isolationLevel) {
        return beginTransaction(RunMode.CLIENT_SERVER, isolationLevel);
    }

    default Transaction beginTransaction(RunMode runMode) {
        return beginTransaction(runMode, Transaction.IL_READ_COMMITTED);
    }

    default Transaction beginTransaction(RunMode runMode, int isolationLevel) {
        return beginTransaction(runMode, isolationLevel, null);
    }

    Transaction beginTransaction(RunMode runMode, int isolationLevel, Scheduler scheduler);

    boolean supportsMVCC();

    void checkpoint();

    default Runnable getFsyncService() {
        return null;
    }

    default boolean containsRepeatableReadTransactions() {
        return false;
    }

    default List<? extends Transaction> currentTransactions() {
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
