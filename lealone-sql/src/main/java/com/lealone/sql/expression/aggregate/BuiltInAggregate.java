/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.sql.expression.aggregate;

import java.util.HashMap;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.util.StringUtils;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.index.Index;
import com.lealone.db.result.SortOrder;
import com.lealone.db.row.SearchRow;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Column;
import com.lealone.db.table.Table;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.ExpressionColumn;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.optimizer.TableFilter;
import com.lealone.sql.query.Select;

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

    public int getAType() {
        return type;
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

    public AggregateData getAggregateData() {
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
                SearchRow row = index.findFirstOrLast(session, first);
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

    public boolean isOptimizable(Table table) {
        switch (type) {
        case COUNT:
            if (!distinct && on.getNullable() == Column.NOT_NULLABLE) {
                return table.canGetRowCount();
            }
            return false;
        case COUNT_ALL:
            return table.canGetRowCount();
        case MIN:
        case MAX:
            Index index = getColumnIndex();
            return index != null;
        default:
            return false;
        }
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

    protected String getSQL(String text) {
        if (distinct) {
            return text + "(DISTINCT " + on.getSQL() + ")";
        }
        return text + StringUtils.enclose(on.getSQL());
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitAggregate(this);
    }
}
