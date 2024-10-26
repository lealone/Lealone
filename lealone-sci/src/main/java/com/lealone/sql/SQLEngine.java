/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import com.lealone.db.Constants;
import com.lealone.db.PluggableEngine;
import com.lealone.db.PluginManager;
import com.lealone.db.command.CommandParameter;
import com.lealone.db.session.Session;
import com.lealone.db.value.Value;

public interface SQLEngine extends PluggableEngine {

    public static SQLEngine getDefaultSQLEngine() {
        return PluginManager.getPlugin(SQLEngine.class, Constants.DEFAULT_SQL_ENGINE_NAME);
    }

    SQLParser createParser(Session session);

    String quoteIdentifier(String identifier);

    CommandParameter createParameter(int index);

    IExpression createValueExpression(Value value);

    IExpression createSequenceValue(Object sequence);

    IExpression createConditionAndOr(boolean and, IExpression left, IExpression right);

}
