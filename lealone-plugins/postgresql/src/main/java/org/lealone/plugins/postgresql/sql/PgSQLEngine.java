/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.sql;

import org.lealone.db.CommandParameter;
import org.lealone.db.PluginBase;
import org.lealone.db.schema.Sequence;
import org.lealone.db.session.Session;
import org.lealone.db.value.Value;
import org.lealone.plugins.postgresql.server.PgServerEngine;
import org.lealone.sql.IExpression;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLParser;
import org.lealone.sql.expression.Parameter;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.condition.ConditionAndOr;

public class PgSQLEngine extends PluginBase implements SQLEngine {

    public PgSQLEngine() {
        super(PgServerEngine.NAME);
    }

    @Override
    public SQLParser createParser(Session session) {
        return new PgSQLParser((org.lealone.db.session.ServerSession) session);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return PgSQLParser.quoteIdentifier(identifier);
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
        return new ConditionAndOr(and ? ConditionAndOr.AND : ConditionAndOr.OR,
                (org.lealone.sql.expression.Expression) left,
                (org.lealone.sql.expression.Expression) right);
    }
}
