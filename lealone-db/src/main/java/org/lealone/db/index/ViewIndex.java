/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.index;

import java.util.ArrayList;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.Utils;
import org.lealone.db.CommandParameter;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.result.Row;
import org.lealone.db.result.SearchRow;
import org.lealone.db.result.SortOrder;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;
import org.lealone.db.util.IntArray;
import org.lealone.db.util.SynchronizedVerifier;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.sql.IQuery;
import org.lealone.sql.ISelectUnion;

/**
 * This object represents a virtual index for a query.
 * Actually it only represents a prepared SELECT statement.
 */
public class ViewIndex extends IndexBase {

    private final TableView view;
    private final String querySQL;
    private final ArrayList<CommandParameter> originalParameters;
    private final SmallLRUCache<IntArray, CostElement> costCache = SmallLRUCache
            .newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private boolean recursive;
    private final int[] indexMasks;
    private String planSQL;
    private IQuery query;
    private final ServerSession createSession;

    public ViewIndex(TableView view, String querySQL, ArrayList<CommandParameter> originalParameters,
            boolean recursive) {
        super(view, 0, null, IndexType.createNonUnique(), null);
        this.view = view;
        this.querySQL = querySQL;
        this.originalParameters = originalParameters;
        this.recursive = recursive;
        columns = new Column[0];
        this.createSession = null;
        this.indexMasks = null;
    }

    public ViewIndex(TableView view, ViewIndex index, ServerSession session, int[] masks) {
        super(view, 0, null, IndexType.createNonUnique(), null);
        this.view = view;
        this.querySQL = index.querySQL;
        this.originalParameters = index.originalParameters;
        this.recursive = index.recursive;
        this.indexMasks = masks;
        this.createSession = session;
        columns = new Column[0];
        if (!recursive) {
            query = getQuery(session, masks);
            planSQL = query.getPlanSQL();
        }
    }

    public ServerSession getSession() {
        return createSession;
    }

    public void setRecursive(boolean value) {
        this.recursive = value;
    }

    public boolean isRecursive() {
        return recursive;
    }

    @Override
    public String getPlanSQL() {
        return planSQL;
    }

    /**
     * A calculated cost value.
     */
    static class CostElement {

        /**
         * The time in milliseconds when this cost was calculated.
         */
        long evaluatedAt;

        /**
         * The cost.
         */
        double cost;
    }

    @Override
    public synchronized double getCost(ServerSession session, int[] masks, SortOrder sortOrder) {
        if (recursive) {
            return 1000;
        }
        IntArray masksArray = new IntArray(masks == null ? Utils.EMPTY_INT_ARRAY : masks);
        SynchronizedVerifier.check(costCache);
        CostElement cachedCost = costCache.get(masksArray);
        if (cachedCost != null) {
            long time = System.currentTimeMillis();
            if (time < cachedCost.evaluatedAt + Constants.VIEW_COST_CACHE_MAX_AGE) {
                return cachedCost.cost;
            }
        }
        IQuery q = (IQuery) session.prepareStatement(querySQL, true);
        if (masks != null) {
            IntArray paramIndex = new IntArray();
            for (int i = 0; i < masks.length; i++) {
                int mask = masks[i];
                if (mask == 0) {
                    continue;
                }
                paramIndex.add(i);
            }
            int len = paramIndex.size();
            for (int i = 0; i < len; i++) {
                int idx = paramIndex.get(i);
                int mask = masks[idx];
                int nextParamIndex = q.getParameters().size() + view.getParameterOffset();
                if ((mask & IndexConditionType.EQUALITY) != 0) {
                    CommandParameter param = session.getDatabase().getSQLEngine()
                            .createParameter(nextParamIndex);
                    q.addGlobalCondition(param, idx, IndexConditionType.EQUALITY);
                } else {
                    if ((mask & IndexConditionType.START) != 0) {
                        CommandParameter param = session.getDatabase().getSQLEngine()
                                .createParameter(nextParamIndex);
                        q.addGlobalCondition(param, idx, IndexConditionType.START);
                    }
                    if ((mask & IndexConditionType.END) != 0) {
                        CommandParameter param = session.getDatabase().getSQLEngine()
                                .createParameter(nextParamIndex);
                        q.addGlobalCondition(param, idx, IndexConditionType.END);
                    }
                }
            }
            String sql = q.getPlanSQL();
            q = (IQuery) session.prepareStatement(sql, true);
        }
        double cost = q.getCost();
        cachedCost = new CostElement();
        cachedCost.evaluatedAt = System.currentTimeMillis();
        cachedCost.cost = cost;
        costCache.put(masksArray, cachedCost);
        return cost;
    }

    @Override
    public Cursor find(ServerSession session, SearchRow first, SearchRow last) {
        if (recursive) {
            Result recResult = view.getRecursiveResult();
            if (recResult != null) {
                recResult.reset();
                return new ViewCursor(this, recResult, first, last);
            }
            if (query == null) {
                query = (IQuery) createSession.prepareStatement(querySQL, true);
                planSQL = query.getPlanSQL();
            }
            if (!(query instanceof ISelectUnion)) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_2, "recursive queries without UNION ALL");
            }
            ISelectUnion union = (ISelectUnion) query;
            if (union.getUnionType() != ISelectUnion.UNION_ALL) {
                throw DbException.get(ErrorCode.SYNTAX_ERROR_2, "recursive queries without UNION ALL");
            }
            IQuery left = union.getLeft();
            // to ensure the last result is not closed
            left.disableCache();
            LocalResult r = (LocalResult) left.query(0);
            LocalResult result = (LocalResult) union.getEmptyResult();
            while (r.next()) {
                result.addRow(r.currentRow());
            }
            IQuery right = union.getRight();
            r.reset();
            view.setRecursiveResult(r);
            // to ensure the last result is not closed
            right.disableCache();
            while (true) {
                r = (LocalResult) right.query(0);
                if (r.getRowCount() == 0) {
                    break;
                }
                while (r.next()) {
                    result.addRow(r.currentRow());
                }
                r.reset();
                view.setRecursiveResult(r);

                // 避免死循环，因为此时union all的右边子句不是当前view
                if (!right.getTables().contains(view)) {
                    break;
                }
            }
            view.setRecursiveResult(null);
            result.done();
            return new ViewCursor(this, result, first, last);
        }
        List<? extends CommandParameter> paramList = query.getParameters();
        if (originalParameters != null) {
            for (int i = 0, size = originalParameters.size(); i < size; i++) {
                CommandParameter orig = originalParameters.get(i);
                int idx = orig.getIndex();
                Value value = orig.getValue();
                setParameter(paramList, idx, value);
            }
        }
        int len;
        if (first != null) {
            len = first.getColumnCount();
        } else if (last != null) {
            len = last.getColumnCount();
        } else {
            len = 0;
        }
        int idx = originalParameters == null ? 0 : originalParameters.size();
        idx += view.getParameterOffset();
        for (int i = 0; i < len; i++) {
            int mask = indexMasks[i];
            if ((mask & IndexConditionType.EQUALITY) != 0) {
                setParameter(paramList, idx++, first.getValue(i));
            }
            if ((mask & IndexConditionType.START) != 0) {
                setParameter(paramList, idx++, first.getValue(i));
            }
            if ((mask & IndexConditionType.END) != 0) {
                setParameter(paramList, idx++, last.getValue(i));
            }
        }
        Result result = query.query(0);
        return new ViewCursor(this, result, first, last);
    }

    private static void setParameter(List<? extends CommandParameter> paramList, int x, Value v) {
        if (x >= paramList.size()) {
            // the parameter may be optimized away as in
            // select * from (select null as x) where x=1;
            return;
        }
        CommandParameter param = paramList.get(x);
        param.setValue(v);
    }

    private IQuery getQuery(ServerSession session, int[] masks) {
        IQuery q = (IQuery) session.prepareStatement(querySQL, true);
        if (masks == null) {
            return q;
        }
        if (!q.allowGlobalConditions()) {
            return q;
        }
        int firstIndexParam = originalParameters == null ? 0 : originalParameters.size();
        firstIndexParam += view.getParameterOffset();
        IntArray paramIndex = new IntArray();
        int indexColumnCount = 0;
        for (int i = 0; i < masks.length; i++) {
            int mask = masks[i];
            if (mask == 0) {
                continue;
            }
            indexColumnCount++;
            paramIndex.add(i);
            if (Integer.bitCount(mask) > 1) {
                // two parameters for range queries: >= x AND <= y
                paramIndex.add(i);
            }
        }
        int len = paramIndex.size();
        ArrayList<Column> columnList = new ArrayList<>(len);
        for (int i = 0; i < len;) {
            int idx = paramIndex.get(i);
            columnList.add(table.getColumn(idx));
            int mask = masks[idx];
            if ((mask & IndexConditionType.EQUALITY) == IndexConditionType.EQUALITY) {
                CommandParameter param = session.getDatabase().getSQLEngine()
                        .createParameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, IndexConditionType.EQUALITY);
                i++;
            }
            if ((mask & IndexConditionType.START) == IndexConditionType.START) {
                CommandParameter param = session.getDatabase().getSQLEngine()
                        .createParameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, IndexConditionType.START);
                i++;
            }
            if ((mask & IndexConditionType.END) == IndexConditionType.END) {
                CommandParameter param = session.getDatabase().getSQLEngine()
                        .createParameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, IndexConditionType.END);
                i++;
            }
        }
        columns = new Column[columnList.size()];
        columnList.toArray(columns);

        // reconstruct the index columns from the masks
        this.indexColumns = new IndexColumn[indexColumnCount];
        this.columnIds = new int[indexColumnCount];
        for (int type = 0, indexColumnId = 0; type < 2; type++) {
            for (int i = 0; i < masks.length; i++) {
                int mask = masks[i];
                if (mask == 0) {
                    continue;
                }
                if (type == 0) {
                    if ((mask & IndexConditionType.EQUALITY) != IndexConditionType.EQUALITY) {
                        // the first columns need to be equality conditions
                        continue;
                    }
                } else {
                    if ((mask & IndexConditionType.EQUALITY) == IndexConditionType.EQUALITY) {
                        // then only range conditions
                        continue;
                    }
                }
                IndexColumn c = new IndexColumn();
                c.column = table.getColumn(i);
                indexColumns[indexColumnId] = c;
                columnIds[indexColumnId] = c.column.getColumnId();
                indexColumnId++;
            }
        }

        String sql = q.getPlanSQL();
        q = (IQuery) session.prepareStatement(sql, true);
        return q;
    }

    @Override
    public long getRowCount(ServerSession session) {
        return 0;
    }

    @Override
    public long getRowCountApproximation() {
        return 0;
    }

    /**
     * The cursor implementation of a view index.
     */
    private static class ViewCursor implements Cursor {

        private final Table table;
        private final Index index;
        private final Result result;
        private final SearchRow first, last;
        private Row current;

        ViewCursor(Index index, Result result, SearchRow first, SearchRow last) {
            this.table = index.getTable();
            this.index = index;
            this.result = result;
            this.first = first;
            this.last = last;
        }

        @Override
        public Row get() {
            return current;
        }

        @Override
        public boolean next() {
            while (true) {
                boolean res = result.next();
                if (!res) {
                    result.close();
                    current = null;
                    return false;
                }
                current = table.getTemplateRow();
                Value[] values = result.currentRow();
                for (int i = 0, len = current.getColumnCount(); i < len; i++) {
                    Value v = i < values.length ? values[i] : ValueNull.INSTANCE;
                    current.setValue(i, v);
                }
                int comp;
                if (first != null) {
                    comp = index.compareRows(current, first);
                    if (comp < 0) {
                        continue;
                    }
                }
                if (last != null) {
                    comp = index.compareRows(current, last);
                    if (comp > 0) {
                        continue;
                    }
                }
                return true;
            }
        }
    }
}
