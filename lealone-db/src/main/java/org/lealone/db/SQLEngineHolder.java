/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import org.lealone.sql.SQLEngine;

public class SQLEngineHolder {

    private static SQLEngine sqlEngine = PluginManager.getPlugin(SQLEngine.class,
            Constants.DEFAULT_SQL_ENGINE_NAME);

    static void setSQLEngine(SQLEngine sqlEngine) {
        SQLEngineHolder.sqlEngine = sqlEngine;
    }

    public static String quoteIdentifier(String identifier) {
        return sqlEngine.quoteIdentifier(identifier);
    }

}
