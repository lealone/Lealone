/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote;

public class AOTransactionEngine extends AMTransactionEngine {

    private static final String NAME = "AOTE";

    public AOTransactionEngine() {
        super(NAME);
    }
}
