/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLEngineManager;

public class SQLEngineHolder {

    private static SQLEngine sqlEngine = SQLEngineManager.getInstance().getEngine(Constants.DEFAULT_SQL_ENGINE_NAME);

    static void setSQLEngine(SQLEngine sqlEngine) {
        SQLEngineHolder.sqlEngine = sqlEngine;
    }

    public static String quoteIdentifier(String identifier) {
        return sqlEngine.quoteIdentifier(identifier);
    }

}
