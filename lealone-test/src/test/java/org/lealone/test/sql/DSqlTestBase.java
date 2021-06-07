/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql;

import org.lealone.db.RunMode;

//Distributed SqlTestBase
//需要启动集群才能测试
public class DSqlTestBase extends SqlTestBase {

    protected DSqlTestBase() {
    }

    protected DSqlTestBase(String dbName) {
        super(dbName);
    }

    protected DSqlTestBase(String dbName, RunMode runMode) {
        super(dbName, runMode);
    }

    protected DSqlTestBase(String user, String password) {
        super(user, password);
    }

    @Override
    protected boolean autoStartTcpServer() {
        return false;
    }
}
