/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db;

import org.lealone.db.PluginManager;
import org.lealone.db.session.ServerSessionFactory;
import org.lealone.db.session.SessionFactory;
import org.lealone.test.UnitTestBase;

public abstract class DbTestBase extends UnitTestBase {

    public static ServerSessionFactory getServerSessionFactory() {
        return (ServerSessionFactory) PluginManager.getPlugin(SessionFactory.class,
                ServerSessionFactory.class.getSimpleName());
    }
}
