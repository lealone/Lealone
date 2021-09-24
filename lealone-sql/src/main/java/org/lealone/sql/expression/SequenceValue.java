/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression;

import org.lealone.db.schema.Sequence;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.sql.expression.visitor.ExpressionVisitor;
import org.lealone.sql.vector.SingleValueVector;
import org.lealone.sql.vector.ValueVector;

/**
 * Wraps a sequence when used in a statement.
 */
public class SequenceValue extends Expression {

    private final Sequence sequence;

    public SequenceValue(Sequence sequence) {
        this.sequence = sequence;
    }

    public Sequence getSequence() {
        return sequence;
    }

    @Override
    public Value getValue(ServerSession session) {
        long value = sequence.getNext(session);
        session.setLastIdentity(ValueLong.get(value));
        return ValueLong.get(value);
    }

    @Override
    public ValueVector getValueVector(ServerSession session, ValueVector bvv) {
        return new SingleValueVector(getValue(session));
    }

    @Override
    public int getType() {
        return Value.LONG;
    }

    @Override
    public Expression optimize(ServerSession session) {
        return this;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public long getPrecision() {
        return ValueInt.PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return ValueInt.DISPLAY_SIZE;
    }

    @Override
    public String getSQL(boolean isDistributed) {
        return "(NEXT VALUE FOR " + sequence.getSQL() + ")";
    }

    @Override
    public int getCost() {
        return 1;
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitSequenceValue(this);
    }
}
