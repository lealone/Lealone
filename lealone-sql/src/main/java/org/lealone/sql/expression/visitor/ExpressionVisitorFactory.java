/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.visitor;

import java.util.Set;

import org.lealone.db.DbObject;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.sql.optimizer.ColumnResolver;

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

    /**
    * Create a new visitor to check if no expression depends on the given
    * resolver.
    *
    * @param resolver the resolver
    * @return the new visitor
    */
    public static NotFromResolverVisitor getNotFromResolverVisitor(ColumnResolver resolver) {
        return new NotFromResolverVisitor(resolver);
    }

    /**
     * The visitor singleton for the type QUERY_COMPARABLE.
     */
    private static final QueryComparableVisitor QUERY_COMPARABLE_VISITOR = new QueryComparableVisitor();

    /**
     * Can the expression be added to a condition of an outer query.
     * Example: ROWNUM() can't be added as a condition to the inner query of
     * select id from (select t.*, rownum as r from test t) where r between 2 and 3;
     * Also a sequence expression must not be used.
     */
    public static QueryComparableVisitor getQueryComparableVisitor() {
        return QUERY_COMPARABLE_VISITOR;
    }

    /**
     * The visitor singleton for the type EVALUATABLE.
     */
    private static final EvaluatableVisitor EVALUATABLE_VISITOR = new EvaluatableVisitor();

    /**
     * Can the expression be evaluated, that means are all columns set to 'evaluatable'?
     */
    public static EvaluatableVisitor getEvaluatableVisitor() {
        return EVALUATABLE_VISITOR;
    }

    /**
     * The visitor singleton for the type DETERMINISTIC.
     */
    private static final DeterministicVisitor DETERMINISTIC_VISITOR = new DeterministicVisitor();

    /**
     * Does the expression return the same results for the same parameters?
     */
    public static DeterministicVisitor getDeterministicVisitor() {
        return DETERMINISTIC_VISITOR;
    }

    /**
     * The visitor singleton for the type INDEPENDENT.
     */
    private static final IndependentVisitor INDEPENDENT_VISITOR = new IndependentVisitor();

    /**
     * Is the value independent on unset parameters or on columns of a higher
     * level query, or sequence values (that means can it be evaluated right now)?
     */
    public static IndependentVisitor getIndependentVisitor() {
        return INDEPENDENT_VISITOR;
    }

    /**
     * Create a new visitor to check if all aggregates are for the given table.
     *
     * @param table the table
     * @return the new visitor
     */
    public static OptimizableVisitor getOptimizableVisitor(Table table) {
        return new OptimizableVisitor(table);
    }
}
