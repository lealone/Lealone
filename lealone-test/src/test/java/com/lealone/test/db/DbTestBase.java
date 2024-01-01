/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db;

import com.lealone.db.PluginManager;
import com.lealone.db.session.ServerSessionFactory;
import com.lealone.db.session.SessionFactory;
import com.lealone.test.UnitTestBase;

public abstract class DbTestBase extends UnitTestBase {

    public static ServerSessionFactory getServerSessionFactory() {
        return (ServerSessionFactory) PluginManager.getPlugin(SessionFactory.class,
                ServerSessionFactory.class.getSimpleName());
    }
}
