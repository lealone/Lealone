/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.PluggableEngineBase;

public abstract class TransactionEngineBase extends PluggableEngineBase implements TransactionEngine {

    public TransactionEngineBase(String name) {
        super(name);
    }

    @Override
    public boolean supportsMVCC() {
        return false;
    }

    @Override
    public void checkpoint() {
    }
}
