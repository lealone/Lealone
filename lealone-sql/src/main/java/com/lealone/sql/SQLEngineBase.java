/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql;

import com.lealone.db.Plugin;
import com.lealone.db.PluginBase;
import com.lealone.db.command.CommandParameter;
import com.lealone.db.schema.Sequence;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.Session;
import com.lealone.db.value.Value;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.Parameter;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.expression.ValueExpression;
import com.lealone.sql.expression.condition.ConditionAndOr;

public abstract class SQLEngineBase extends PluginBase implements SQLEngine {

    public SQLEngineBase(String name) {
        super(name);
    }

    public abstract SQLParserBase createParser(ServerSession session);

    @Override
    public SQLParserBase createParser(Session session) {
        return createParser((ServerSession) session);
    }

    @Override
    public CommandParameter createParameter(int index) {
        return new Parameter(index);
    }

    @Override
    public IExpression createValueExpression(Value value) {
        return ValueExpression.get(value);
    }

    @Override
    public IExpression createSequenceValue(Object sequence) {
        return new SequenceValue((Sequence) sequence);
    }

    @Override
    public IExpression createConditionAndOr(boolean and, IExpression left, IExpression right) {
        return new ConditionAndOr(and ? ConditionAndOr.AND : ConditionAndOr.OR, (Expression) left,
                (Expression) right);
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return SQLEngine.class;
    }
}
