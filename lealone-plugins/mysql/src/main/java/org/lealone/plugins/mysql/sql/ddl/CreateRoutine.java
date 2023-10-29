/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.sql.ddl;

import java.util.List;

import org.lealone.common.util.Utils;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.sql.StatementBase;
import org.lealone.sql.ddl.SchemaStatement;
import org.lealone.sql.expression.Expression;

public abstract class CreateRoutine extends SchemaStatement {

    protected String name;
    protected boolean deterministic;
    protected boolean ifNotExists;
    protected final List<Column> parameters = Utils.newSmallArrayList();

    public CreateRoutine(ServerSession session, Schema schema) {
        super(session, schema);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDeterministic(boolean deterministic) {
        this.deterministic = deterministic;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void addParameter(Column parameter) {
        parameters.add(parameter);
    }

    public void setExpression(Expression expression) {
    }

    public void setPrepared(StatementBase prep) {
    }
}
