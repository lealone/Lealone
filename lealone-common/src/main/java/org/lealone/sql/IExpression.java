/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

import java.util.Set;

import org.lealone.db.session.Session;
import org.lealone.db.value.Value;

public interface IExpression {

    interface Evaluator {

        IExpression optimizeExpression(Session session, IExpression e);

        Value getExpressionValue(Session session, IExpression e, Object data);
    }

    int getType();

    String getSQL();

    IExpression optimize(Session session);

    Value getValue(Session session);

    String getAlias();

    String getSchemaName();

    String getTableName();

    String getColumnName();

    int getDisplaySize();

    long getPrecision();

    int getScale();

    int getNullable();

    boolean isAutoIncrement();

    boolean isConstant();

    IExpression getNonAliasExpression();

    void getDependencies(Set<?> dependencies);

    void getColumns(Set<?> columns);
}
