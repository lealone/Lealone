/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.visitor;

import java.util.Set;

import com.lealone.db.table.Column;
import com.lealone.sql.expression.ExpressionColumn;

public class ColumnsVisitor extends VoidExpressionVisitor {

    private Set<Column> columns;

    public ColumnsVisitor(Set<Column> columns) {
        this.columns = columns;
    }

    @Override
    public Void visitExpressionColumn(ExpressionColumn e) {
        columns.add(e.getColumn());
        return null;
    }
}
