/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import java.util.HashMap;

import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.visitor.ExpressionVisitor;
import org.lealone.sql.query.Select;

/**
 * This class is used by the built-in aggregate functions,
 * as well as the user-defined aggregate functions.
 *
 * @author H2 Group
 * @author zhh
 */
public abstract class Aggregate extends Expression {

    /**
     * The aggregate type for COUNT(*).
     */
    public static final int COUNT_ALL = 0;

    /**
     * The aggregate type for COUNT(expression).
     */
    public static final int COUNT = 1;

    /**
     * The aggregate type for GROUP_CONCAT(...).
     */
    public static final int GROUP_CONCAT = 2;

    /**
     * The aggregate type for SUM(expression).
     */
    public static final int SUM = 3;

    /**
     * The aggregate type for MIN(expression).
     */
    public static final int MIN = 4;

    /**
     * The aggregate type for MAX(expression).
     */
    public static final int MAX = 5;

    /**
     * The aggregate type for AVG(expression).
     */
    public static final int AVG = 6;

    /**
     * The aggregate type for STDDEV_POP(expression).
     */
    public static final int STDDEV_POP = 7;

    /**
     * The aggregate type for STDDEV_SAMP(expression).
     */
    public static final int STDDEV_SAMP = 8;

    /**
     * The aggregate type for VAR_POP(expression).
     */
    public static final int VAR_POP = 9;

    /**
     * The aggregate type for VAR_SAMP(expression).
     */
    public static final int VAR_SAMP = 10;

    /**
     * The aggregate type for BOOL_OR(expression).
     */
    public static final int BOOL_OR = 11;

    /**
     * The aggregate type for BOOL_AND(expression).
     */
    public static final int BOOL_AND = 12;

    /**
     * The aggregate type for BIT_OR(expression).
     */
    public static final int BIT_OR = 13;

    /**
     * The aggregate type for BIT_AND(expression).
     */
    public static final int BIT_AND = 14;

    /**
     * The aggregate type for SELECTIVITY(expression).
     */
    public static final int SELECTIVITY = 15;

    /**
     * The aggregate type for HISTOGRAM(expression).
     */
    public static final int HISTOGRAM = 16;

    private static final HashMap<String, Integer> AGGREGATES = new HashMap<>();

    static {
        addAggregate("COUNT", COUNT);
        addAggregate("SUM", SUM);
        addAggregate("MIN", MIN);
        addAggregate("MAX", MAX);
        addAggregate("AVG", AVG);
        addAggregate("GROUP_CONCAT", GROUP_CONCAT);
        // PostgreSQL compatibility: string_agg(expression, delimiter)
        addAggregate("STRING_AGG", GROUP_CONCAT);
        addAggregate("STDDEV_SAMP", STDDEV_SAMP);
        addAggregate("STDDEV", STDDEV_SAMP);
        addAggregate("STDDEV_POP", STDDEV_POP);
        addAggregate("STDDEVP", STDDEV_POP);
        addAggregate("VAR_POP", VAR_POP);
        addAggregate("VARP", VAR_POP);
        addAggregate("VAR_SAMP", VAR_SAMP);
        addAggregate("VAR", VAR_SAMP);
        addAggregate("VARIANCE", VAR_SAMP);
        addAggregate("BOOL_OR", BOOL_OR);
        addAggregate("BOOL_AND", BOOL_AND);
        // HSQLDB compatibility, but conflicts with x > SOME(...)
        addAggregate("SOME", BOOL_OR);
        // HSQLDB compatibility, but conflicts with x > EVERY(...)
        addAggregate("EVERY", BOOL_AND);
        addAggregate("SELECTIVITY", SELECTIVITY);
        addAggregate("HISTOGRAM", HISTOGRAM);
        addAggregate("BIT_OR", BIT_OR);
        addAggregate("BIT_AND", BIT_AND);
    }

    private static void addAggregate(String name, int type) {
        AGGREGATES.put(name, type);
    }

    /**
     * Get the aggregate type for this name, or -1 if no aggregate has been found.
     *
     * @param name the aggregate function name
     * @return -1 if no aggregate function has been found, or the aggregate type
     */
    public static int getAggregateType(String name) {
        Integer type = AGGREGATES.get(name);
        return type == null ? -1 : type.intValue();
    }

    public static Aggregate create(int type, Expression on, Select select, boolean distinct) {
        switch (type) {
        case Aggregate.COUNT:
            return new ACount(type, on, select, distinct);
        case Aggregate.COUNT_ALL:
            return new ACountAll(type, on, select, distinct);
        case Aggregate.GROUP_CONCAT:
            return new AGroupConcat(type, on, select, distinct);
        case Aggregate.HISTOGRAM:
            return new AHistogram(type, on, select, distinct);
        case Aggregate.SELECTIVITY:
            return new ASelectivity(type, on, select, distinct);
        default:
            return new ADefault(type, on, select, distinct);
        }
    }

    protected final Select select;
    protected int dataType;
    protected int lastGroupRowId;

    public Aggregate(Select select) {
        this.select = select;
    }

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitAggregate(this);
    }

    public Expression getOn() {
        return null;
    }
}
