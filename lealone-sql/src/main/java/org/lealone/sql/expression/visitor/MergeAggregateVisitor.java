/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.aggregate.AGroupConcat;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.sql.expression.aggregate.JavaAggregate;

public class MergeAggregateVisitor extends ExpressionVisitorBase<Void> {

    private ServerSession session;
    private Value value;

    public MergeAggregateVisitor(ServerSession session, Value value) {
        this.session = session;
        this.value = value;
    }

    @Override
    public Void visitAggregate(Aggregate e) {
        e.mergeAggregate(session, value);
        return null;
    }

    @Override
    public Void visitAGroupConcat(AGroupConcat e) {
        e.mergeAggregate(session, value);
        return null;
    }

    @Override
    public Void visitJavaAggregate(JavaAggregate e) {
        e.mergeAggregate(session, value);
        return null;
    }
}
