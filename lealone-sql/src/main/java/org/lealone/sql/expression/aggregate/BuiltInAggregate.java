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
import org.lealone.sql.optimizer.TableFilter;
import org.lealone.sql.query.Select;
import org.lealone.sql.vector.SingleValueVector;
import org.lealone.sql.vector.ValueVector;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 * 
 * @author H2 Group
 * @author zhh
 */
public abstract class BuiltInAggregate extends Aggregate {

    protected final int type;
    protected final boolean distinct;

    protected Expression on;
    protected int scale;
    protected long precision;
    protected int displaySize;

    /**
     * Create a new aggregate object.
     *
     * @param type the aggregate type
     * @param on the aggregated expression
     * @param select the select statement
     * @param distinct if distinct is used
     */
    public BuiltInAggregate(int type, Expression on, Select select, boolean distinct) {
        super(select);
        this.type = type;
        this.on = on;
        this.distinct = distinct;
    }

    @Override
    public Expression getOn() {
        return on;
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
    public int getCost() {
        return (on == null) ? 1 : on.getCost() + 1;
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

    protected abstract AggregateData createAggregateData();

    private AggregateData getAggregateData() {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            // this is a different level (the enclosing query)
            return null;
        }

        int groupRowId = select.getCurrentGroupRowId();
        if (lastGroupRowId == groupRowId) {
            // already visited
            return null;
        }
        lastGroupRowId = groupRowId;

        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
            group.put(this, data);
        }
        return data;
    }

    @Override
    public void updateAggregate(ServerSession session) {
        AggregateData data = getAggregateData();
        if (data == null) {
            return;
        }
        Value v = on == null ? null : on.getValue(session);
        data.add(session, v);
    }

    @Override
    public void updateVectorizedAggregate(ServerSession session, ValueVector bvv) {
        AggregateData data = getAggregateData();
        if (data == null) {
            return;
        }
        ValueVector vv = on == null ? null : on.getValueVector(session);
        data.add(session, bvv, vv);
    }

    @Override
    public void mergeAggregate(ServerSession session, Value v) {
        AggregateData data = getAggregateData();
        if (data == null) {
            return;
        }
        data.merge(session, v);
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
        return getFinalAggregateData().getValue(session);
    }

    private AggregateData getFinalAggregateData() {
        HashMap<Expression, Object> group = select.getCurrentGroup();
        if (group == null) {
            throw DbException.get(ErrorCode.INVALID_USE_OF_AGGREGATE_FUNCTION_1, getSQL());
        }
        AggregateData data = (AggregateData) group.get(this);
        if (data == null) {
            data = createAggregateData();
        }
        return data;
    }

    @Override
    public ValueVector getValueVector(ServerSession session) {
        return new SingleValueVector(getValue(session));
    }

    @Override
    public Value getMergedValue(ServerSession session) {
        return getFinalAggregateData().getMergedValue(session);
    }

    @Override
    public void calculate(Calculator calculator) {
        switch (type) {
        case BuiltInAggregate.COUNT_ALL:
        case BuiltInAggregate.COUNT:
        case BuiltInAggregate.MIN:
        case BuiltInAggregate.MAX:
        case BuiltInAggregate.SUM:
        case BuiltInAggregate.BOOL_AND:
        case BuiltInAggregate.BOOL_OR:
        case BuiltInAggregate.BIT_AND:
        case BuiltInAggregate.BIT_OR:
            break;
        case BuiltInAggregate.AVG: {
            int i = calculator.getIndex();
            Value v = divide(calculator.getValue(i + 1), calculator.getValue(i).getLong());
            calculator.addResultValue(v);
            calculator.addIndex(2);
            break;
        }
        case BuiltInAggregate.STDDEV_POP: {
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = Math.sqrt(sum2 / count - (sum1 / count) * (sum1 / count));
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case BuiltInAggregate.STDDEV_SAMP: { // 见:http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = Math.sqrt((sum2 - (sum1 * sum1 / count)) / (count - 1));
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case BuiltInAggregate.VAR_POP: {
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = sum2 / count - (sum1 / count) * (sum1 / count);
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case BuiltInAggregate.VAR_SAMP: {
            int i = calculator.getIndex();
            long count = calculator.getValue(i).getLong();
            double sum1 = calculator.getValue(i + 1).getDouble();
            double sum2 = calculator.getValue(i + 2).getDouble();
            double result = (sum2 - (sum1 * sum1 / count)) / (count - 1);
            calculator.addResultValue(ValueDouble.get(result));
            calculator.addIndex(3);
            break;
        }
        case BuiltInAggregate.HISTOGRAM:
        case BuiltInAggregate.SELECTIVITY:
        case BuiltInAggregate.GROUP_CONCAT:
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
