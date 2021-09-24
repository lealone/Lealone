/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.db.session.ServerSession;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;

public class UpdateAggregateVisitor extends VoidExpressionVisitor {

    private ServerSession session;

    public UpdateAggregateVisitor(ServerSession session) {
        this.session = session;
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        e.updateAggregate(session);
        return null;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        e.updateAggregate(session);
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        e.updateAggregate(session);
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        e.updateAggregate(session);
        return null;
    }
}
