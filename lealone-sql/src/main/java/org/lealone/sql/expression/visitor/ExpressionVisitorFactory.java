/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import java.util.Set;

import org.lealone.db.DbObject;
import org.lealone.db.table.Column;

public class ExpressionVisitorFactory {

    /**
     * Create a new visitor to get all referenced columns.
     *
     * @param columns the columns map
     * @return the new visitor
     */
    public static ColumnsVisitor getColumnsVisitor(Set<Column> columns) {
        return new ColumnsVisitor(columns);
    }

    /**
     * Create a new visitor object to collect dependencies.
     *
     * @param dependencies the dependencies set
     * @return the new visitor
     */
    public static DependenciesVisitor getDependenciesVisitor(Set<DbObject> dependencies) {
        return new DependenciesVisitor(dependencies);
    }

    public static MaxModificationIdVisitor getMaxModificationIdVisitor() {
        return new MaxModificationIdVisitor();
    }
}
