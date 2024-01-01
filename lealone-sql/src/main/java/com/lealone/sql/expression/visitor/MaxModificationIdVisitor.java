/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.SequenceValue;
import com.lealone.sql.optimizer.TableFilter;
import com.lealone.sql.query.Query;

public class MaxModificationIdVisitor extends VoidExpressionVisitor {

    private long maxDataModificationId;

    public long getMaxDataModificationId() {
        return maxDataModificationId;
    }

    public void setMaxDataModificationId(long maxDataModificationId) {
        if (maxDataModificationId > this.maxDataModificationId) {
            this.maxDataModificationId = maxDataModificationId;
        }
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        setMaxDataModificationId(e.getColumn().getTable().getMaxDataModificationId());
        return null;
    }

    @Override
    public Void visitSequenceValue(SequenceValue e) {
        setMaxDataModificationId(e.getSequence().getModificationId());
        return null;
    }

    @Override
    protected Void visitQuery(Query query) {
        super.visitQuery(query);
        for (int i = 0, size = query.getFilters().size(); i < size; i++) {
            TableFilter f = query.getFilters().get(i);
            long m = f.getTable().getMaxDataModificationId();
            setMaxDataModificationId(m);
        }
        return null;
    }
}
