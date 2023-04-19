/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.PluggableEngine;

public interface TransactionEngine extends PluggableEngine {

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
}
