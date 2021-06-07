/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.PluggableEngineManager;

public class TransactionEngineManager extends PluggableEngineManager<TransactionEngine> {

    private static final TransactionEngineManager instance = new TransactionEngineManager();

    public static TransactionEngineManager getInstance() {
        return instance;
    }

    private TransactionEngineManager() {
        super(TransactionEngine.class);
    }

    public static TransactionEngine getTransactionEngine(String name) {
        return instance.getEngine(name);
    }

}
