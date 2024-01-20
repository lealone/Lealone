/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db;

import com.lealone.db.session.ServerSession;
import com.lealone.db.session.ServerSessionFactory;
import com.lealone.test.UnitTestBase;

public abstract class DbTestBase extends UnitTestBase {

    public static ServerSession createServerSession(String url) {
        return (ServerSession) ServerSessionFactory.getInstance().createSession(url).get();
    }
}
