/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.PluggableEngine;
import org.lealone.db.RunMode;

public interface TransactionEngine extends PluggableEngine {

    default Transaction beginTransaction(boolean autoCommit) {
        return beginTransaction(autoCommit, RunMode.CLIENT_SERVER);
    }

    Transaction beginTransaction(boolean autoCommit, RunMode runMode);

    boolean validateTransaction(String globalTransactionName);

    boolean supportsMVCC();

    TransactionMap<?, ?> getTransactionMap(String mapName, Transaction transaction);

    void checkpoint();
}
