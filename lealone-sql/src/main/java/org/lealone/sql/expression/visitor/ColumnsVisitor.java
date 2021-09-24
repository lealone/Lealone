/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import java.util.Set;

import org.lealone.db.table.Column;
import org.lealone.sql.expression.ExpressionColumn;

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
