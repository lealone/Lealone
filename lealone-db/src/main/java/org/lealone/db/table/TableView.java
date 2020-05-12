/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.table;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.SmallLRUCache;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.CommandParameter;
import org.lealone.db.Constants;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.auth.User;
import org.lealone.db.index.Index;
import org.lealone.db.index.ViewIndex;
import org.lealone.db.lock.DbObjectLock;
import org.lealone.db.result.LocalResult;
import org.lealone.db.result.Result;
import org.lealone.db.schema.Schema;
import org.lealone.db.session.ServerSession;
import org.lealone.db.util.IntArray;
import org.lealone.db.util.SynchronizedVerifier;
import org.lealone.db.value.Value;
import org.lealone.sql.IExpression;
import org.lealone.sql.IQuery;
import org.lealone.sql.PreparedSQLStatement;

/**
 * A view is a virtual table that is defined by a query.
 */
public class TableView extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100;

    private String querySQL;
    private ArrayList<Table> tables;
    private String[] columnNames;
    private IQuery viewQuery;
    private ViewIndex index;
    private boolean recursive;
    private DbException createException;
    private final SmallLRUCache<IntArray, ViewIndex> indexCache = SmallLRUCache
            .newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private long lastModificationCheck;
    private long maxDataModificationId;
    private User owner;
    private IQuery topQuery;
    private LocalResult recursiveResult;
    private boolean tableExpression;

    public TableView(Schema schema, int id, String name, String querySQL, ArrayList<CommandParameter> params,
            String[] columnNames, ServerSession session, boolean recursive) {
        super(schema, id, name, false, true);
        init(querySQL, params, columnNames, session, recursive);
    }

    /**
     * Try to replace the SQL statement of the view and re-compile this and all
     * dependent views.
     *
     * @param querySQL the SQL statement
     * @param columnNames the column names
     * @param session the session
     * @param recursive whether this is a recursive view
     * @param force if errors should be ignored
     */
    public void replace(String querySQL, String[] columnNames, ServerSession session, boolean recursive,
            boolean force) {
        String oldQuerySQL = this.querySQL;
        String[] oldColumnNames = this.columnNames;
        boolean oldRecursive = this.recursive;
        init(querySQL, null, columnNames, session, recursive);
        DbException e = recompile(session, force);
        if (e != null) {
            init(oldQuerySQL, null, oldColumnNames, session, oldRecursive);
            recompile(session, true);
            throw e;
        }
    }

    private synchronized void init(String querySQL, ArrayList<CommandParameter> params, String[] columnNames,
            ServerSession session, boolean recursive) {
        this.querySQL = querySQL;
        this.columnNames = columnNames;
        this.recursive = recursive;
        index = new ViewIndex(this, querySQL, params, recursive);
        SynchronizedVerifier.check(indexCache);
        indexCache.clear();
        initColumnsAndTables(session);
    }

    private static IQuery compileViewQuery(ServerSession session, String sql) {
        PreparedSQLStatement p = session.prepareStatement(sql);
        p = p.getWrappedStatement(); // 要看最原始的那条语句的类型
        if (!(p instanceof IQuery)) {
            throw DbException.getSyntaxError(sql, 0);
        }
        return (IQuery) p;
    }

    /**
     * Re-compile the view query and all views that depend on this object.
     *
     * @param session the session
     * @param force if exceptions should be ignored
     * @return the exception if re-compiling this or any dependent view failed
     *         (only when force is disabled)
     */
    public synchronized DbException recompile(ServerSession session, boolean force) {
        try {
            compileViewQuery(session, querySQL);
        } catch (DbException e) {
            if (!force) {
                return e;
            }
        }
        ArrayList<TableView> views = getViews();
        if (views != null) {
            views = new ArrayList<>(views);
        }
        SynchronizedVerifier.check(indexCache);
        indexCache.clear();
        initColumnsAndTables(session);
        if (views != null) {
            for (TableView v : views) {
                DbException e = v.recompile(session, force);
                if (e != null && !force) {
                    return e;
                }
            }
        }
        return force ? null : createException;
    }

    @SuppressWarnings("unchecked")
    private void initColumnsAndTables(ServerSession session) {
        Column[] cols;
        removeViewFromTables();
        try {
            IQuery query = compileViewQuery(session, querySQL);
            this.querySQL = query.getPlanSQL();
            tables = new ArrayList<>((HashSet<Table>) query.getTables());
            List<? extends IExpression> expressions = query.getExpressions();
            int count = query.getColumnCount();
            ArrayList<Column> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                IExpression expr = expressions.get(i);
                String name = null;
                if (columnNames != null && columnNames.length > i) {
                    name = columnNames[i];
                }
                if (name == null) {
                    name = expr.getAlias();
                }
                int type = expr.getType();
                long precision = expr.getPrecision();
                int scale = expr.getScale();
                int displaySize = expr.getDisplaySize();
                Column col = new Column(name, type, precision, scale, displaySize);
                col.setTable(this, i);
                list.add(col);
            }
            cols = new Column[list.size()];
            list.toArray(cols);
            createException = null;
            viewQuery = query;
        } catch (DbException e) {
            e.addSQL(getCreateSQL());
            createException = e;
            // if it can't be compiled, then it's a 'zero column table'
            // this avoids problems when creating the view when opening the
            // database
            tables = Utils.newSmallArrayList();
            cols = new Column[0];
            if (recursive && columnNames != null) {
                cols = new Column[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    cols[i] = new Column(columnNames[i], Value.STRING);
                }
                index.setRecursive(true);
                createException = null;
            }
        }
        setColumns(cols);
        if (getId() != 0) {
            addViewToTables();
        }
    }

    /**
     * Check if this view is currently invalid.
     *
     * @return true if it is
     */
    public boolean isInvalid() {
        return createException != null;
    }

    // @Override
    // public PlanItem getBestPlanItem(ServerSession session, int[] masks, TableFilter filter, SortOrder sortOrder) {
    // PlanItem item = new PlanItem();
    // item.cost = index.getCost(session, masks, filter, sortOrder);
    // IntArray masksArray = new IntArray(masks == null ? Utils.EMPTY_INT_ARRAY : masks);
    // SynchronizedVerifier.check(indexCache);
    // ViewIndex i2 = indexCache.get(masksArray);
    // if (i2 == null || i2.getSession() != session) {
    // i2 = new ViewIndex(this, index, session, masks);
    // indexCache.put(masksArray, i2);
    // }
    // item.setIndex(i2);
    // return item;
    // }

    @Override
    public String getDropSQL() {
        return "DROP VIEW IF EXISTS " + getSQL() + " CASCADE";
    }

    @Override
    public String getCreateSQL() {
        return getCreateSQL(false, true);
    }

    /**
     * Generate "CREATE" SQL statement for the view.
     *
     * @param orReplace if true, then include the OR REPLACE clause
     * @param force if true, then include the FORCE clause
     * @return the SQL statement
     */
    public String getCreateSQL(boolean orReplace, boolean force) {
        return getCreateSQL(orReplace, force, getSQL());
    }

    private String getCreateSQL(boolean orReplace, boolean force, String quotedName) {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (orReplace) {
            buff.append("OR REPLACE ");
        }
        if (force) {
            buff.append("FORCE ");
        }
        buff.append("VIEW ");
        buff.append(quotedName);
        if (comment != null) {
            buff.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        if (columns != null && columns.length > 0) {
            buff.append('(');
            for (Column c : columns) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');
        } else if (columnNames != null) {
            buff.append('(');
            for (String n : columnNames) {
                buff.appendExceptFirst(", ");
                buff.append(n);
            }
            buff.append(')');
        }
        return buff.append(" AS\n").append(querySQL).toString();
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public TableType getTableType() {
        return TableType.VIEW;
    }

    @Override
    public Index getScanIndex(ServerSession session) {
        if (createException != null) {
            String msg = createException.getMessage();
            throw DbException.get(ErrorCode.VIEW_IS_INVALID_2, createException, getSQL(), msg);
        }
        // PlanItem item = getBestPlanItem(session, null, null, null);
        // return item.getIndex();

        IntArray masksArray = new IntArray(Utils.EMPTY_INT_ARRAY);
        SynchronizedVerifier.check(indexCache);
        ViewIndex i2 = indexCache.get(masksArray);
        if (i2 == null || i2.getSession() != session) {
            i2 = new ViewIndex(this, index, session, null);
            indexCache.put(masksArray, i2);
        }
        return i2;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public long getMaxDataModificationId() {
        if (createException != null) {
            return Long.MAX_VALUE;
        }
        if (viewQuery == null) {
            return Long.MAX_VALUE;
        }
        // if nothing was modified in the database since the last check, and the
        // last is known, then we don't need to check again
        // this speeds up nested views
        long dbMod = database.getModificationDataId();
        if (dbMod > lastModificationCheck && maxDataModificationId <= dbMod) {
            maxDataModificationId = viewQuery.getMaxDataModificationId();
            lastModificationCheck = dbMod;
        }
        return maxDataModificationId;
    }

    @Override
    public boolean isDeterministic() {
        if (recursive || viewQuery == null) {
            return false;
        }
        return viewQuery.isDeterministic();
    }

    @Override
    public boolean canDrop() {
        return true;
    }

    @Override
    public boolean canGetRowCount() {
        // TODO view: could get the row count, but not that easy
        return false;
    }

    @Override
    public long getRowCount(ServerSession session) {
        throw DbException.throwInternalError();
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    @Override
    public void removeChildrenAndResources(ServerSession session, DbObjectLock lock) {
        removeViewFromTables();
        super.removeChildrenAndResources(session, lock);
    }

    @Override
    public void invalidate() {
        querySQL = null;
        index = null;
        super.invalidate();
    }

    @Override
    public String getSQL() {
        if (isTemporary()) {
            return "(\n" + StringUtils.indent(querySQL) + ")";
        }
        return super.getSQL();
    }

    public String getQuery() {
        return querySQL;
    }

    private void removeViewFromTables() {
        if (tables != null) {
            for (Table t : tables) {
                t.removeView(this);
            }
            tables.clear();
        }
    }

    private void addViewToTables() {
        for (Table t : tables) {
            t.addView(this);
        }
    }

    private void setOwner(User owner) {
        this.owner = owner;
    }

    public User getOwner() {
        return owner;
    }

    /**
     * Create a temporary view out of the given query.
     *
     * @param session the session
     * @param owner the owner of the query
     * @param name the view name
     * @param query the query
     * @param topQuery the top level query
     * @return the view table
     */
    public static TableView createTempView(ServerSession session, User owner, String name, IQuery query,
            IQuery topQuery) {
        Schema mainSchema = session.getDatabase().getSchema(session, Constants.SCHEMA_MAIN);
        String querySQL = query.getPlanSQL();
        int size = query.getParameters().size();
        ArrayList<CommandParameter> parms = new ArrayList<CommandParameter>(size);
        for (CommandParameter p : query.getParameters())
            parms.add(p);
        TableView v = new TableView(mainSchema, 0, name, querySQL, parms, null, session, false);
        if (v.createException != null) {
            throw v.createException;
        }
        v.setTopQuery(topQuery);
        v.setOwner(owner);
        v.setTemporary(true);
        return v;
    }

    private void setTopQuery(IQuery topQuery) {
        this.topQuery = topQuery;
    }

    public int getParameterOffset() {
        return topQuery == null ? 0 : topQuery.getParameters().size();
    }

    public void setRecursiveResult(LocalResult value) {
        if (recursiveResult != null) {
            recursiveResult.close();
        }
        this.recursiveResult = value;
    }

    public Result getRecursiveResult() {
        return recursiveResult;
    }

    public void setTableExpression(boolean tableExpression) {
        this.tableExpression = tableExpression;
    }

    public boolean isTableExpression() {
        return tableExpression;
    }

    public String getTableName() {
        for (Table t : tables) {
            if (t instanceof TableView) {
                return ((TableView) t).getTableName();
            } else {
                return t.getName();
            }
        }

        return getName();
    }

}
