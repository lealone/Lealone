/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.expression.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.lealone.common.util.StatementBuilder;
import org.lealone.db.Database;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.ValueHashMap;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionVisitor;
import org.lealone.sql.expression.SelectOrderBy;
import org.lealone.sql.optimizer.ColumnResolver;
import org.lealone.sql.query.Select;

public class AGroupConcat extends Aggregate {

    private Expression groupConcatSeparator;
    private ArrayList<SelectOrderBy> groupConcatOrderList;
    private SortOrder groupConcatSort;

    public AGroupConcat(int type, Expression on, Select select, boolean distinct) {
        super(type, on, select, distinct);
    }

    public Expression getGroupConcatSeparator() {
        return groupConcatSeparator;
    }

    public ArrayList<SelectOrderBy> getGroupConcatOrderList() {
        return groupConcatOrderList;
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
    public void mapColumns(ColumnResolver resolver, int level) {
        super.mapColumns(resolver, level);
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
        super.optimize(session);
        if (groupConcatOrderList != null) {
            for (SelectOrderBy o : groupConcatOrderList) {
                o.expression = o.expression.optimize(session);
            }
            groupConcatSort = initOrder(session);
        }
        if (groupConcatSeparator != null) {
            groupConcatSeparator = groupConcatSeparator.optimize(session);
        }
        dataType = Value.STRING;
        scale = 0;
        precision = displaySize = Integer.MAX_VALUE;
        return this;
    }

    @Override
    protected AggregateData createAggregateData() {
        return new AggregateDataGroupConcat();
    }

    @Override
    protected void add(ServerSession session, AggregateData data, Value v) {
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
        data.add(session.getDatabase(), v);
    }

    @Override
    protected Value getValue(ServerSession session, AggregateData data, Value v) {
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
        return v;
    }

    @Override
    public String getSQL(boolean isDistributed) {
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
    public boolean isEverything(ExpressionVisitor visitor) {
        if (!super.isEverything(visitor)) {
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

    private class AggregateDataGroupConcat extends AggregateData {

        private ArrayList<Value> list;
        private ValueHashMap<AggregateDataGroupConcat> distinctValues;

        @Override
        void add(Database database, Value v) {
            add(database, v, distinct);
        }

        private void add(Database database, Value v, boolean distinct) {
            if (v == ValueNull.INSTANCE) {
                return;
            }
            if (distinct) {
                if (distinctValues == null) {
                    distinctValues = ValueHashMap.newInstance();
                }
                distinctValues.put(v, this);
                return;
            }
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(v);
        }

        @Override
        Value getValue(Database database) {
            if (distinct) {
                groupDistinct(database, dataType);
            }
            return null;
        }

        ArrayList<Value> getList() {
            return list;
        }

        private void groupDistinct(Database database, int dataType) {
            if (distinctValues == null) {
                return;
            }
            for (Value v : distinctValues.keys()) {
                add(database, v, false);
            }
        }

        @Override
        void merge(Database database, Value v) {
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(v);
        }

        @Override
        Value getMergedValue(Database database) {
            return null;
        }
    }
}
