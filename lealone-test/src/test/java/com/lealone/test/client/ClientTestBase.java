/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.client;

import com.lealone.test.sql.SqlTestBase;

public abstract class ClientTestBase extends SqlTestBase {

    protected ClientTestBase() {
    }

    protected ClientTestBase(String dbName) {
        super(dbName);
    }
}
