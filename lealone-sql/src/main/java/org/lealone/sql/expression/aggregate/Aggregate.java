/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import java.util.HashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.expression.Calculator;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.visitor.IExpressionVisitor;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;
import org.lealone.sql.vector.ValueVector;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
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
    static final int SUM = 3;

    /**
     * The aggregate type for MIN(expression).
     */
    static final int MIN = 4;

    /**
     * The aggregate type for MAX(expression).
     */
    static final int MAX = 5;

    /**
     * The aggregate type for AVG(expression).
     */
    static final int AVG = 6;

    /**
     * The aggregate type for STDDEV_POP(expression).
     */
    static final int STDDEV_POP = 7;

    /**
     * The aggregate type for STDDEV_SAMP(expression).
     */
    static final int STDDEV_SAMP = 8;

    /**
     * The aggregate type for VAR_POP(expression).
     */
    static final int VAR_POP = 9;

    /**
     * The aggregate type for VAR_SAMP(expression).
     */
    static final int VAR_SAMP = 10;

    /**
     * The aggregate type for BOOL_OR(expression).
     */
    static final int BOOL_OR = 11;

    /**
     * The aggregate type for BOOL_AND(expression).
     */
    static final int BOOL_AND = 12;

    /**
     * The aggregate type for BOOL_OR(expression).
     */
    static final int BIT_OR = 13;

    /**
     * The aggregate type for BOOL_AND(expression).
     */
    static final int BIT_AND = 14;

    /**
     * The aggregate type for SELECTIVITY(expression).
     */
    static final int SELECTIVITY = 15;

    /**
     * The aggregate type for HISTOGRAM(expression).
     */
    static final int HISTOGRAM = 16;

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
     * Get the aggregate type for this name, or -1 if no aggregate has been
     * found.
     *
     * @param name the aggregate function name
     * @return -1 if no aggregate function has been found, or the aggregate type
     */
    public static int getAggregateType(String name) {
        Integer type = AGGREGATES.get(name);
        return type == null ? -1 : type.intValue();
    }

    public static Aggregate create(int type, Expression on, Select select, boolean distinct) {
        if (type == Aggregate.SELECTIVITY) {
            return new ASelectivity(type, on, select, distinct);
        } else if (type == Aggregate.GROUP_CONCAT) {
            return new AGroupConcat(type, on, select, distinct);
        } else if (type == Aggregate.COUNT_ALL) {
            return new ACountAll(type, on, select, distinct);
        } else if (type == Aggregate.COUNT) {
            return new ACount(type, on, select, distinct);
        } else if (type == Aggregate.HISTOGRAM) {
            return new AHistogram(type, on, select, distinct);
        } else {
            return new ADefault(type, on, select, distinct);
        }
    }

    protected final int type;
    protected final Select select;
    protected final boolean distinct;

    protected Expression on;
    protected int dataType, scale;
    protected long precision;
    protected int displaySize;
    private int lastGroupRowId;

    /**
     * Create a new aggregate object.
     *
     * @param type the aggregate type
     * @param on the aggregated expression
     * @param select the select statement
     * @param distinct if distinct is used
     */
    public Aggregate(int type, Expression on, Select select, boolean distinct) {
        this.type = type;
        this.on = on;
        this.select = select;
        this.distinct = distinct;
    }

    public Expression getOn() {
        return on;
    }

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public int getScale() {
        return scale;
    }

    @Override
    public long getPrecision() {
        return precision;
    }

    @Override
    public int getDisplaySize() {
        return displaySize;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (on != null) {
            on.mapColumns(resolver, level);
        }
    }

    @Override
    public Expression optimize(ServerSession session) {
        if (on != null) {
            on = on.optimize(session);
            dataType = on.getType();
            scale = on.getScale();
            precision = on.getPrecision();
            displaySize = on.getDisplaySize();
        }
        return this;
    }

    @Override
    public void updateAggregate(ServerSession session) {
        // TODO aggregates: check nested MIN(MAX(ID)) and so on
        // if (on != null) {
        // on.updateAggregate();
        // }
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
            group.put(this, data);
        }
        Value v = on == null ? null : on.getValue(session);
        add(session, data, v);
    }

    protected abstract AggregateData createAggregateData();

    protected void add(ServerSession session, AggregateData data, Value v) {
        data.add(session.getDatabase(), dataType, distinct, v);
    }

    @Override
    public void updateAggregate(ServerSession session, ValueVector bvv) {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
            group.put(this, data);
        }
        ValueVector vv = on == null ? null : on.getValueVector(session);
        add(session, data, bvv, vv);
    }

    protected void add(ServerSession session, AggregateData data, ValueVector bvv, ValueVector vv) {
        data.add(session.getDatabase(), dataType, distinct, bvv, vv);
    }

    @Override
    public void mergeAggregate(ServerSession session, Value v) {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return;
        }
        lastGroupRowId = groupRowId;

        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
            group.put(this, data);
        }
        data.merge(session.getDatabase(), dataType, distinct, v);
    }

    @Override
    public Value getValue(ServerSession session) {
        if (select.isQuickAggregateQuery()) {
            switch (type) {
            case COUNT:
            case COUNT_ALL:
                Table table = select.getTopTableFilter().getTable();
                return ValueLong.get(table.getRowCount(session));
            case MIN:
            case MAX:
                boolean first = type == MIN;
                Index index = getColumnIndex();
                int sortType = index.getIndexColumns()[0].sortType;
                if ((sortType & SortOrder.DESCENDING) != 0) {
                    first = !first;
                }
                Cursor cursor = index.findFirstOrLast(session, first);
                SearchRow row = cursor.getSearchRow();
                Value v;
                if (row == null) {
                    v = ValueNull.INSTANCE;
                } else {
                    v = row.getValue(index.getColumns()[0].getColumnId());
                }
                return v;
            default:
                DbException.throwInternalError("type=" + type);
            }
        }
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
        }
        Value v = data.getValue(session.getDatabase(), dataType, distinct);
        return getValue(session, data, v);
    }

    protected Value getValue(ServerSession session, AggregateData data, Value v) {
        return v;
    }

    @Override
    public Value getMergedValue(ServerSession session) {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
        }
        return data.getMergedValue(session.getDatabase(), dataType, distinct);
    }

    @Override
    public void calculate(Calculator calculator) {
        switch (type) {
        case Aggregate.COUNT_ALL:
        case Aggregate.COUNT:
        case Aggregate.MIN:
        case Aggregate.MAX:
        case Aggregate.SUM:
        case Aggregate.BOOL_AND:
        case Aggregate.BOOL_OR:
        case Aggregate.BIT_AND:
        case Aggregate.BIT_OR:
            break;
        case Aggregate.AVG: {
            int i = calculator.getIndex();
            Value v = divide(calculator.getValue(i + 1), calculator.getValue(i).getLong());
            calculator.addResultValue(v);
            calculator.addIndex(2);
            break;
        }
        case Aggregate.STDDEV_POP: {
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = Math.sqrt(sum2 / count - (sum1 / count) * (sum1 / count));
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case Aggregate.STDDEV_SAMP: { // 见:http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = Math.sqrt((sum2 - (sum1 * sum1 / count)) / (count - 1));
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case Aggregate.VAR_POP: {
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = sum2 / count - (sum1 / count) * (sum1 / count);
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case Aggregate.VAR_SAMP: {
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = (sum2 - (sum1 * sum1 / count)) / (count - 1);
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case Aggregate.HISTOGRAM:
        case Aggregate.SELECTIVITY:
        case Aggregate.GROUP_CONCAT:
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
    }

    private Index getColumnIndex() {
        if (on instanceof ExpressionColumn) {
            ExpressionColumn col = (ExpressionColumn) on;
            Column column = col.getColumn();
            TableFilter filter = col.getTableFilter();
            if (filter != null) {
                Table table = filter.getTable();
                Index index = table.getIndexForColumn(column);
                return index;
            }
        }
        return null;
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        if (visitor.getType() == ExpressionVisitor.OPTIMIZABLE_MIN_MAX_COUNT_ALL) {
            switch (type) {
            case COUNT:
                if (!distinct && on.getNullable() == Column.NOT_NULLABLE) {
                    return visitor.getTable().canGetRowCount();
                }
                return false;
            case COUNT_ALL:
                return visitor.getTable().canGetRowCount();
            case MIN:
            case MAX:
                Index index = getColumnIndex();
                return index != null;
            default:
                return false;
            }
        }
        if (on != null && !on.isEverything(visitor)) {
            return false;
        }
        return true;
    }

    @Override
    public int getCost() {
        return (on == null) ? 1 : on.getCost() + 1;
    }

    static Value divide(Value a, long by) {
        if (by == 0) {
            return ValueNull.INSTANCE;
        }
        int type = Value.getHigherOrder(a.getType(), Value.LONG);
        Value b = ValueLong.get(by).convertTo(type);
        a = a.convertTo(type).divide(b);
        return a;
    }

    protected String getSQL(String text, boolean isDistributed) {
        if (distinct) {
            return text + "(DISTINCT " + on.getSQL(isDistributed) + ")";
        }
        return text + StringUtils.enclose(on.getSQL(isDistributed));
    }

    @Override
    public <R> R accept(IExpressionVisitor<R> visitor) {
        return visitor.visitAggregate(this);
    }
}
