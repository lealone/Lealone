/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

import org.lealone.db.PluggableEngineManager;

public class SQLEngineManager extends PluggableEngineManager<SQLEngine> {

    private static final SQLEngineManager instance = new SQLEngineManager();

    public static SQLEngineManager getInstance() {
        return instance;
    }

    private SQLEngineManager() {
        super(SQLEngine.class);
    }
}
