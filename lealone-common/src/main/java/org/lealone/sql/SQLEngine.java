/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

import org.lealone.db.CommandParameter;
import org.lealone.db.PluggableEngine;
import org.lealone.db.session.Session;
import org.lealone.db.value.Value;

public interface SQLEngine extends PluggableEngine {

    SQLParser createParser(Session session);

    String quoteIdentifier(String identifier);

    CommandParameter createParameter(int index);

    IExpression createValueExpression(Value value);

    IExpression createSequenceValue(Object sequence);

    IExpression createConditionAndOr(boolean and, IExpression left, IExpression right);

}
