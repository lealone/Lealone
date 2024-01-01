/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.expression.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.lealone.common.util.StatementBuilder;
import com.lealone.db.result.SortOrder;
import com.lealone.db.session.ServerSession;
import com.lealone.db.util.ValueHashMap;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueArray;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueString;
import com.lealone.sql.expression.Expression;
import com.lealone.sql.expression.SelectOrderBy;
import com.lealone.sql.expression.visitor.ExpressionVisitor;
import com.lealone.sql.query.Select;

public class AGroupConcat extends BuiltInAggregate {

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
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder("GROUP_CONCAT(");
        if (distinct) {
            buff.append("DISTINCT ");
        }
        buff.append(on.getSQL());
        if (groupConcatOrderList != null) {
            buff.append(" ORDER BY ");
            for (SelectOrderBy o : groupConcatOrderList) {
                buff.appendExceptFirst(", ");
                buff.append(o.expression.getSQL());
                if (o.descending) {
                    buff.append(" DESC");
                }
            }
        }
        if (groupConcatSeparator != null) {
            buff.append(" SEPARATOR ").append(groupConcatSeparator.getSQL());
        }
        return buff.append(')').toString();
    }

    @Override
    public <R> R accept(ExpressionVisitor<R> visitor) {
        return visitor.visitAGroupConcat(this);
    }

    public class AggregateDataGroupConcat extends AggregateData {

        private ArrayList<Value> list;
        private ValueHashMap<AggregateDataGroupConcat> distinctValues;

        @Override
        public void add(ServerSession session, Value v) {
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
            add(session, v, distinct);
        }

        private void add(ServerSession session, Value v, boolean distinct) {
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
        Value getValue(ServerSession session) {
            if (distinct) {
                groupDistinct(session, dataType);
            }
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
            String sep = groupConcatSeparator == null ? ","
                    : groupConcatSeparator.getValue(session).getString();
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
            return ValueString.get(buff.toString());
        }

        private void groupDistinct(ServerSession session, int dataType) {
            if (distinctValues == null) {
                return;
            }
            for (Value v : distinctValues.keys()) {
                add(session, v, false);
            }
        }
    }
}
