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
import org.lealone.sql.vector.ValueVector;

public class UpdateVectorizedAggregateVisitor extends VoidExpressionVisitor {

    private ServerSession session;
    private ValueVector bvv;

    public UpdateVectorizedAggregateVisitor(ServerSession session, ValueVector bvv) {
        this.session = session;
        this.bvv = bvv;
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        e.updateVectorizedAggregate(session, bvv);
        return null;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        e.updateVectorizedAggregate(session, bvv);
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        e.updateVectorizedAggregate(session, bvv);
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        e.updateVectorizedAggregate(session, bvv);
        return null;
    }
}
