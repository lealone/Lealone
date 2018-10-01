/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.ServerSession;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.index.Cursor;
import org.lealone.db.index.Index;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.dml.Select;
import org.lealone.sql.expression.Calculator;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.SelectOrderBy;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.optimizer.TableFilter;

/**
 * Implements the integrated aggregate functions, such as COUNT, MAX, SUM.
 */
public class Aggregate extends Expression {

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

    private final int type;
    private final Select select;
    private final boolean distinct;

    private Expression on;
    private Expression groupConcatSeparator;
    private ArrayList<SelectOrderBy> groupConcatOrderList;
    private SortOrder groupConcatSort;
    private int dataType, scale;
    private long precision;
    private int displaySize;
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
        // HSQLDB compatibility, but conflicts with x > EVERY(...)
        addAggregate("SOME", BOOL_OR);
        addAggregate("BOOL_AND", BOOL_AND);
        // HSQLDB compatibility, but conflicts with x > SOME(...)
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

    /**
     * Set the order for GROUP_CONCAT() aggregate.
     *
     * @param orderBy the order by list
     */
    public void setGroupConcatOrder(ArrayList<SelectOrderBy> orderBy) {
        this.groupConcatOrderList = orderBy;
    }

    /**
     * Set the separator for the GROUP_CONCAT() aggregate.
     *
     * @param separator the separator expression
     */
    public void setGroupConcatSeparator(Expression separator) {
        this.groupConcatSeparator = separator;
    }

    private SortOrder initOrder(ServerSession session) {
        int size = groupConcatOrderList.size();
        int[] index = new int[size];
        int[] sortType = new int[size];
        for (int i = 0; i < size; i++) {
            SelectOrderBy o = groupConcatOrderList.get(i);
            index[i] = i + 1;
            int order = o.descending ? SortOrder.DESCENDING : SortOrder.ASCENDING;
            sortType[i] = order;
        }
        return new SortOrder(session.getDatabase(), index, sortType, null);
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
            data = AggregateData.create(type);
            group.put(this, data);
        }
        Value v = on == null ? null : on.getValue(session);
        if (type == GROUP_CONCAT) {
            if (v != ValueNull.INSTANCE) {
                v = v.convertTo(Value.STRING);
                if (groupConcatOrderList != null) {
                    int size = groupConcatOrderList.size();
                    Value[] array = new Value[1 + size];
                    array[0] = v;
                    for (int i = 0; i < size; i++) {
                        SelectOrderBy o = groupConcatOrderList.get(i);
                        array[i + 1] = o.expression.getValue(session);
                    }
                    v = ValueArray.get(array);
                }
            }
        }
        data.add(session.getDatabase(), dataType, distinct, v);
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
            data = AggregateData.create(type);
            group.put(this, data);
        }
        if (type == GROUP_CONCAT) {
            if (v != ValueNull.INSTANCE) {
                v = v.convertTo(Value.STRING);
                if (groupConcatOrderList != null) {
                    int size = groupConcatOrderList.size();
                    Value[] array = new Value[1 + size];
                    array[0] = v;
                    for (int i = 0; i < size; i++) {
                        SelectOrderBy o = groupConcatOrderList.get(i);
                        array[i + 1] = o.expression.getValue(session);
                    }
                    v = ValueArray.get(array);
                }
            }
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
            data = AggregateData.create(type);
        }
        Value v = data.getValue(session.getDatabase(), dataType, distinct);
        if (type == GROUP_CONCAT) {
            ArrayList<Value> list = ((AggregateDataGroupConcat) data).getList();
            if (list == null || list.isEmpty()) {
                return ValueNull.INSTANCE;
            }
            if (groupConcatOrderList != null) {
                final SortOrder sortOrder = groupConcatSort;
                Collections.sort(list, new Comparator<Value>() {
                    @Override
                    public int compare(Value v1, Value v2) {
                        Value[] a1 = ((ValueArray) v1).getList();
                        Value[] a2 = ((ValueArray) v2).getList();
                        return sortOrder.compare(a1, a2);
                    }
                });
            }
            StatementBuilder buff = new StatementBuilder();
            String sep = groupConcatSeparator == null ? "," : groupConcatSeparator.getValue(session).getString();
            for (Value val : list) {
                String s;
                if (val.getType() == Value.ARRAY) {
                    s = ((ValueArray) val).getList()[0].getString();
                } else {
                    s = val.getString();
                }
                if (s == null) {
                    continue;
                }
                if (sep != null) {
                    buff.appendExceptFirst(sep);
                }
                buff.append(s);
            }
            v = ValueString.get(buff.toString());
        }
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
            data = AggregateData.create(type);
        }
        Value v = data.getMergedValue(session.getDatabase(), dataType, distinct);
        if (type == GROUP_CONCAT) {
            ArrayList<Value> list = ((AggregateDataGroupConcat) data).getList();
            if (list == null || list.isEmpty()) {
                return ValueNull.INSTANCE;
            }
            if (groupConcatOrderList != null) {
                final SortOrder sortOrder = groupConcatSort;
                Collections.sort(list, new Comparator<Value>() {
                    @Override
                    public int compare(Value v1, Value v2) {
                        Value[] a1 = ((ValueArray) v1).getList();
                        Value[] a2 = ((ValueArray) v2).getList();
                        return sortOrder.compare(a1, a2);
                    }
                });
            }
            StatementBuilder buff = new StatementBuilder();
            String sep = groupConcatSeparator == null ? "," : groupConcatSeparator.getValue(session).getString();
            for (Value val : list) {
                String s;
                if (val.getType() == Value.ARRAY) {
                    s = ((ValueArray) val).getList()[0].getString();
                } else {
                    s = val.getString();
                }
                if (s == null) {
                    continue;
                }
                if (sep != null) {
                    buff.appendExceptFirst(sep);
                }
                buff.append(s);
            }
            v = ValueString.get(buff.toString());
        }
        return v;
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

    @Override
    public int getType() {
        return dataType;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        if (on != null) {
            on.mapColumns(resolver, level);
        }
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression.mapColumns(resolver, level);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.mapColumns(resolver, level);
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
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression = o.expression.optimize(session);
            }
            groupConcatSort = initOrder(session);
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator = groupConcatSeparator.optimize(session);
        }
        switch (type) {
        case GROUP_CONCAT:
            dataType = Value.STRING;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case COUNT_ALL:
        case COUNT:
            dataType = Value.LONG;
            scale = 0;
            precision = ValueLong.PRECISION;
            displaySize = ValueLong.DISPLAY_SIZE;
            break;
        case SELECTIVITY:
            dataType = Value.INT;
            scale = 0;
            precision = ValueInt.PRECISION;
            displaySize = ValueInt.DISPLAY_SIZE;
            break;
        case HISTOGRAM:
            dataType = Value.ARRAY;
            scale = 0;
            precision = displaySize = Integer.MAX_VALUE;
            break;
        case SUM:
            if (dataType == Value.BOOLEAN) {
                // example: sum(id > 3) (count the rows)
                dataType = Value.LONG;
            } else if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            } else {
                dataType = DataType.getAddProofType(dataType);
            }
            break;
        case AVG:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        case MIN:
        case MAX:
            break;
        case STDDEV_POP:
        case STDDEV_SAMP:
        case VAR_POP:
        case VAR_SAMP:
            dataType = Value.DOUBLE;
            precision = ValueDouble.PRECISION;
            displaySize = ValueDouble.DISPLAY_SIZE;
            scale = 0;
            break;
        case BOOL_AND:
        case BOOL_OR:
            dataType = Value.BOOLEAN;
            precision = ValueBoolean.PRECISION;
            displaySize = ValueBoolean.DISPLAY_SIZE;
            scale = 0;
            break;
        case BIT_AND:
        case BIT_OR:
            if (!DataType.supportsAdd(dataType)) {
                throw DbException.get(ErrorCode.SUM_OR_AVG_ON_WRONG_DATATYPE_1, getSQL());
            }
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return this;
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        if (on != null) {
            on.setEvaluatable(tableFilter, b);
        }
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression.setEvaluatable(tableFilter, b);
            }
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator.setEvaluatable(tableFilter, b);
        }
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

    private String getSQLGroupConcat(boolean isDistributed) {
        StatementBuilder buff = new StatementBuilder("GROUP_CONCAT(");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(on.getSQL(isDistributed));
        if (groupConcatOrderList != null) {
            buff.append(" ORDER BY ");
            for (SelectOrderBy o : groupConcatOrderList) {
                buff.appendExceptFirst(", ");
                buff.append(o.expression.getSQL(isDistributed));
                if (o.descending) {
                    buff.append(" DESC");
                }
            }
        }
        if (groupConcatSeparator != null) {
            buff.append(" SEPARATOR ").append(groupConcatSeparator.getSQL(isDistributed));
        }
        return buff.append(')').toString();
    }

    @Override
    public String getSQL(boolean isDistributed) {
        String text;
        switch (type) {
        case GROUP_CONCAT:
            return getSQLGroupConcat(isDistributed);
        case COUNT_ALL:
            return "COUNT(*)";
        case COUNT:
            text = "COUNT";
            break;
        case SELECTIVITY:
            text = "SELECTIVITY";
            break;
        case HISTOGRAM:
            text = "HISTOGRAM";
            break;
        case SUM:
            text = "SUM";
            break;
        case MIN:
            text = "MIN";
            break;
        case MAX:
            text = "MAX";
            break;
        case AVG:
            if (isDistributed) {
                if (distinct) {
                    return "COUNT(DISTINCT " + on.getSQL(isDistributed) + "), SUM(DISTINCT " + on.getSQL(isDistributed)
                            + ")";
                } else {
                    return "COUNT(" + on.getSQL(isDistributed) + "), SUM(" + on.getSQL(isDistributed) + ")";
                }
            }
            text = "AVG";
            break;
        case STDDEV_POP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "STDDEV_POP";
            break;
        case STDDEV_SAMP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "STDDEV_SAMP";
            break;
        case VAR_POP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "VAR_POP";
            break;
        case VAR_SAMP:
            if (isDistributed)
                return getSQL_STDDEV_VAR();
            text = "VAR_SAMP";
            break;
        case BOOL_AND:
            text = "BOOL_AND";
            break;
        case BOOL_OR:
            text = "BOOL_OR";
            break;
        case BIT_AND:
            text = "BIT_AND";
            break;
        case BIT_OR:
            text = "BIT_OR";
            break;
        default:
            throw DbException.throwInternalError("type=" + type);
        }
        if (distinct) {
            return text + "(DISTINCT " + on.getSQL(isDistributed) + ")";
        }
        return text + StringUtils.enclose(on.getSQL(isDistributed));
    }

    private String getSQL_STDDEV_VAR() {
        String onSQL = on.getSQL(true);
        if (distinct) {
            return "COUNT(DISTINCT " + onSQL + "), SUM(DISTINCT " + onSQL + "), SUM(DISTINCT " + onSQL + " * " + onSQL
                    + ")";
        } else {
            return "COUNT(" + onSQL + "), SUM(" + onSQL + "), SUM(" + onSQL + " * " + onSQL + ")";
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
        if (groupConcatSeparator != null && !groupConcatSeparator.isEverything(visitor)) {
            return false;
        }
        if (groupConcatOrderList != null) {
            for (int i = 0, size = groupConcatOrderList.size(); i < size; i++) {
                SelectOrderBy o = groupConcatOrderList.get(i);
                if (!o.expression.isEverything(visitor)) {
                    return false;
                }
            }
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
}
