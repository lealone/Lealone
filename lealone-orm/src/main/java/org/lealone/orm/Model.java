/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.orm;

import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.New;
import org.lealone.db.ServerSession;
import org.lealone.db.index.Index;
import org.lealone.db.result.Result;
import org.lealone.db.result.SelectOrderBy;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.orm.property.PBaseNumber;
import org.lealone.sql.dml.Delete;
import org.lealone.sql.dml.Insert;
import org.lealone.sql.dml.Select;
import org.lealone.sql.dml.Update;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.transaction.Transaction;

import com.fasterxml.jackson.databind.JsonNode;

import io.vertx.core.json.JsonObject;

/**
 * Base root model bean.
 * <p>
 * With code generation for each table a model bean is created that extends this.
 * <p>
 * Provides common features for all root model beans
 * </p>
 *
 * @param <T> the model bean type 
 */
@SuppressWarnings("rawtypes")
public abstract class Model<T> {

    public static final short REGULAR_MODEL = 0;
    public static final short ROOT_DAO = 1;
    public static final short CHILD_DAO = 2;

    private static final Logger logger = LoggerFactory.getLogger(Model.class);

    private static final ConcurrentSkipListMap<Long, ServerSession> currentSessions = new ConcurrentSkipListMap<>();
    private static final ConcurrentSkipListMap<Integer, List<ServerSession>> sessionMap = new ConcurrentSkipListMap<>();

    private class PRowId extends PBaseNumber<T, Long> {

        private long value;

        /**
         * Construct with a property name and root instance.
         *
         * @param name property name
         * @param root the root model bean instance
         */
        public PRowId(String name, T root) {
            super(name, root);
        }

        // 不需要通过外部设置
        T set(long value) {
            if (!areEqual(this.value, value)) {
                this.value = value;
                expr().set(name, ValueLong.get(value));
            }
            return root;
        }

        @Override
        public final T deserialize(HashMap<String, Value> map) {
            Value v = map.get(getFullName());
            if (v != null) {
                value = v.getLong();
            }
            return root;
        }

        public final long get() {
            return value;
        }

    }

    private static class NVPair {
        public final String name;
        public final Value value;

        public NVPair(String name, Value value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((name == null) ? 0 : name.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            NVPair other = (NVPair) obj;
            if (name == null) {
                if (other.name != null)
                    return false;
            } else if (!name.equals(other.name))
                return false;
            return true;
        }

    }

    @SuppressWarnings("unchecked")
    private final PRowId _rowid_ = new PRowId(Column.ROWID, (T) this);

    private final ModelTable modelTable;

    /**
     * The root model bean instance. Used to provide fluid query construction.
     */
    private T root;

    // 以下字段不是必须的，所以延迟初始化，避免浪费不必要的内存
    private HashSet<NVPair> nvPairs;
    private ArrayList<Expression> selectExpressions;
    private ArrayList<Expression> groupExpressions;
    private ExpressionBuilder<T> having;
    private ExpressionBuilder<T> whereExpressionBuilder;

    /**
    * The underlying expression builders held as a stack. Pushed and popped based on and/or (conjunction/disjunction).
    */
    private ArrayStack<ExpressionBuilder<T>> expressionBuilderStack;

    private ArrayStack<TableFilter> tableFilterStack;
    private HashMap<String, ModelProperty> modelPropertiesMap;

    ModelProperty[] modelProperties;
    // 0: regular model; 1: root dao; 2: child dao
    short modelType;

    private ArrayList<Model<?>> modelList;
    private HashMap<Class, ArrayList<Model<?>>> modelMap;

    protected Model(ModelTable table, short modelType) {
        this.modelTable = table;
        this.modelType = modelType;
    }

    ModelTable getTable() {
        return modelTable;
    }

    public boolean isDao() {
        return modelType > 0;
    }

    protected void setModelProperties(ModelProperty[] modelProperties) {
        this.modelProperties = modelProperties;
        modelPropertiesMap = new HashMap<>(modelProperties.length);
        for (ModelProperty p : modelProperties) {
            modelPropertiesMap.put(p.getName(), p);
        }
    }

    ModelProperty getModelProperty(String name) {
        return modelPropertiesMap.get(name);
    }

    /**
     * Sets the root model bean instance. Used to provide fluid query construction.
     */
    protected void setRoot(T root) {
        this.root = root;
    }

    protected T addModel(Model<?> m) {
        if (modelList == null) {
            modelList = new ArrayList<>();
        }
        if (modelMap == null) {
            modelMap = new HashMap<>();
        }
        ArrayList<Model<?>> list = modelMap.get(m.getClass());
        if (list == null) {
            list = new ArrayList<>();
            modelMap.put(m.getClass(), list);
        }
        modelList.add(m);
        list.add(m);
        return root;
    }

    @SuppressWarnings("unchecked")
    protected <M> List<M> getModelList(Class c) {
        ArrayList<Model<?>> oldList = modelMap.get(c);
        if (oldList == null) {
            return null;
        }
        ArrayList<Model<?>> newList = new ArrayList<>(oldList.size());
        HashMap<Long, Long> map = new HashMap<>(oldList.size());
        for (Model<?> m : oldList) {
            Long id = m._rowid_.get();
            if (map.put(id, id) == null) {
                newList.add(m);
            }
        }
        return (List<M>) newList;
    }

    void addNVPair(String name, Value value) {
        if (nvPairs == null) {
            nvPairs = new HashSet<>();
        }
        nvPairs.add(new NVPair(name, value));
    }

    private void reset() {
        nvPairs = null;
        selectExpressions = null;
        groupExpressions = null;
        having = null;
        whereExpressionBuilder = null;
        expressionBuilderStack = null;
        tableFilterStack = null;
    }

    private ExpressionBuilder<T> getWhereExpressionBuilder() {
        if (whereExpressionBuilder == null) {
            whereExpressionBuilder = new ExpressionBuilder<T>(this);
        }
        return whereExpressionBuilder;
    }

    private ArrayList<Expression> getSelectExpressions() {
        if (selectExpressions == null) {
            selectExpressions = New.arrayList();
        }
        return selectExpressions;
    }

    public String getDatabaseName() {
        return modelTable.getDatabaseName();
    }

    public String getSchemaName() {
        return modelTable.getSchemaName();
    }

    public String getTableName() {
        return modelTable.getTableName();
    }

    @SafeVarargs
    public final T select(ModelProperty<?>... properties) {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.select(properties);
        }
        selectExpressions = new ArrayList<>();
        for (ModelProperty<?> p : properties) {
            ExpressionColumn c = getExpressionColumn(p);
            selectExpressions.add(c);
        }
        return root;
    }

    public T orderBy() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.orderBy();
        }
        getStack().pop();
        pushExprBuilder(getWhereExpressionBuilder());
        return root;
    }

    @SafeVarargs
    public final T groupBy(ModelProperty<?>... properties) {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.groupBy(properties);
        }
        groupExpressions = new ArrayList<>();
        for (ModelProperty<?> p : properties) {
            ExpressionColumn c = getExpressionColumn(p);
            groupExpressions.add(c);
        }
        return root;
    }

    static ExpressionColumn getExpressionColumn(ModelProperty<?> p) {
        return new ExpressionColumn(p.getDatabaseName(), p.getSchemaName(), p.getTableName(), p.getName());
    }

    static ExpressionColumn getExpressionColumn(TableFilter tableFilter, String propertyName) {
        return new ExpressionColumn(tableFilter.getTable().getDatabase(), tableFilter.getSchemaName(),
                tableFilter.getTableAlias(), propertyName);
    }

    ExpressionColumn getExpressionColumn(String propertyName) {
        return new ExpressionColumn(modelTable.getDatabase(), modelTable.getSchemaName(), modelTable.getTableName(),
                propertyName);
    }

    public T having() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.having();
        }
        getStack().pop();
        having = new ExpressionBuilder<>(this);
        pushExprBuilder(having);
        return root;
    }

    public T or() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.or();
        }
        peekExprBuilder().or();
        return root;
    }

    public T and() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.and();
        }
        peekExprBuilder().and();
        return root;
    }

    public void printSQL() {
        StringBuilder sql = new StringBuilder();
        if (selectExpressions != null) {
            sql.append("SELECT (").append(selectExpressions.get(0).getSQL());
            for (int i = 1, size = selectExpressions.size(); i < size; i++)
                sql.append(", ").append(selectExpressions.get(i).getSQL());
            sql.append(") FROM ").append(modelTable.getTableName());
        }
        if (whereExpressionBuilder != null) {
            sql.append("\r\n  WHERE ").append(whereExpressionBuilder.getExpression().getSQL());
        }
        if (groupExpressions != null) {
            sql.append("\r\n  GROUP BY (").append(groupExpressions.get(0).getSQL());
            for (int i = 1, size = groupExpressions.size(); i < size; i++)
                sql.append(", ").append(groupExpressions.get(i).getSQL());
            sql.append(")");
            if (having != null) {
                sql.append(" HAVING ").append(having.getExpression().getSQL());
            }
        }
        if (whereExpressionBuilder != null) {
            ArrayList<SelectOrderBy> list = whereExpressionBuilder.getOrderList();
            if (list != null && !list.isEmpty()) {
                sql.append("\r\n  ORDER BY (").append(list.get(0).getSQL());
                for (int i = 1, size = list.size(); i < size; i++)
                    sql.append(", ").append(list.get(i).getSQL());
                sql.append(")");
            }
        }
        // reset();
        System.out.println(sql);
    }

    // TODO
    public T not() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.not();
        }
        pushExprBuilder(peekExprBuilder().not());
        return root;
    }

    private void checkDao(String methodName) {
        if (!isDao()) {
            throw new UnsupportedOperationException("The " + methodName + " operation is not allowed, please use "
                    + this.getClass().getSimpleName() + ".dao." + methodName + "() instead.");
        }
    }

    public T where() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.where();
        }
        if (tableFilterStack != null) {
            TableFilter first = tableFilterStack.first();
            while (tableFilterStack.size() > 1) {
                ExpressionBuilder<T> on = getStack().pop();
                TableFilter joined = getTableFilterStack().pop();
                first.addJoin(joined, false, false, on.getExpression());
            }
        }
        return root;
    }

    /**
     * Execute the query returning either a single bean or null (if no matching bean is found).
     */
    public T findOne() {
        return findOne(null);
    }

    public T findOne(Long tid) {
        checkDao("findOne");
        // 进行关联查询时，主表取一条记录，但引用表要取多条
        if (tableFilterStack != null && !tableFilterStack.isEmpty()) {
            List<T> list = findList();
            if (!list.isEmpty()) {
                return list.get(0);
            } else {
                return null;
            }
        }
        Select select = createSelect(tid);
        select.setLimit(ValueExpression.get(ValueInt.get(1)));
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(1);
        result.next();
        reset();
        return deserialize(result, new HashMap<>(1), new ArrayList<>(1));
    }

    private Select createSelect(Long tid) {
        ServerSession session = getSession(tid);
        Select select = new Select(session);
        TableFilter tableFilter;
        if (tableFilterStack != null && !tableFilterStack.isEmpty()) {
            tableFilter = tableFilterStack.peek();
            select.addTableFilter(tableFilter, true);
            boolean selectExpressionsIsNull = false;
            if (selectExpressions == null) {
                selectExpressionsIsNull = true;
                getSelectExpressions().add(new Wildcard(tableFilter.getSchemaName(), tableFilter.getTableAlias()));
            }
            selectExpressions.add(getExpressionColumn(tableFilter, Column.ROWID)); // 总是获取rowid
            while (tableFilter.getJoin() != null) {
                select.addTableFilter(tableFilter.getJoin(), false);
                tableFilter = tableFilter.getJoin();
                if (selectExpressionsIsNull)
                    selectExpressions.add(new Wildcard(tableFilter.getSchemaName(), tableFilter.getTableAlias()));
                selectExpressions.add(getExpressionColumn(tableFilter, Column.ROWID)); // 总是获取rowid
            }
        } else {
            tableFilter = new TableFilter(session, modelTable.getTable(), null, true, null);
            select.addTableFilter(tableFilter, true);
            if (selectExpressions == null) {
                getSelectExpressions().add(new Wildcard(null, null));
            }
            selectExpressions.add(getExpressionColumn(Column.ROWID)); // 总是获取rowid
        }
        select.setExpressions(selectExpressions);
        if (whereExpressionBuilder != null)
            select.addCondition(whereExpressionBuilder.getExpression());
        if (groupExpressions != null) {
            select.setGroupQuery();
            select.setGroupBy(groupExpressions);
            select.setHaving(having.getExpression());
        }
        if (whereExpressionBuilder != null)
            select.setOrder(whereExpressionBuilder.getOrderList());

        return select;
    }

    @SuppressWarnings("unchecked")
    private T deserialize(Result result, HashMap<Long, Model> models, ArrayList<T> list) {
        Value[] row = result.currentRow();
        if (row == null)
            return null;

        int len = row.length;
        HashMap<String, Value> map = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
            String key = result.getSchemaName(i) + "." + result.getTableName(i) + "." + result.getColumnName(i);
            map.put(key, row[i]);
        }

        Model m = newInstance(modelTable, REGULAR_MODEL);

        if (m != null) {
            m._rowid_.deserialize(map);
            Model old = models.get(m._rowid_.get());
            if (old == null) {
                models.put(m._rowid_.get(), m);
                for (ModelProperty p : m.modelProperties) {
                    p.deserialize(map);
                }
                list.add((T) m);
            } else {
                m = old;
            }
        }
        deserializeAssociateInstances(map, m.newAssociateInstances());
        return (T) m;
    }

    @SuppressWarnings("unchecked")
    private void deserializeAssociateInstances(HashMap<String, Value> map, List<Model<?>> associateModels) {
        if (associateModels != null) {
            for (Model associateModel : associateModels) {
                for (ModelProperty p : associateModel.modelProperties) {
                    p.deserialize(map);
                }
                associateModel._rowid_.deserialize(map);
                deserializeAssociateInstances(map, associateModel.newAssociateInstances());
            }
        }
    }

    protected Model newInstance(ModelTable t, short modelType) {
        return null;
    }

    protected List<Model<?>> newAssociateInstances() {
        return null;
    }

    protected void deserialize(JsonNode node) {
        for (ModelProperty p : modelProperties) {
            p.deserialize(node);
        }
    }

    /**
     * Execute the query returning the list of objects.
     */
    public List<T> findList() {
        return findList(null);
    }

    public List<T> findList(Long tid) {
        checkDao("findList");
        Select select = createSelect(tid);
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(-1);
        reset();
        ArrayList<T> list = new ArrayList<>(result.getRowCount());
        HashMap<Long, Model> models = new HashMap<>(result.getRowCount());
        while (result.next()) {
            deserialize(result, models, list);
        }
        return list;
    }

    @SuppressWarnings("unchecked")
    public <M> M m(Model<M> m) {
        Model<T> m2 = maybeCopy();
        if (m2 != this) {
            return m2.m(m);
        }
        Model<T> old = (Model<T>) peekExprBuilder().getOldModel();
        if (!old.isRootDao() && m.getClass() == old.getClass()) {
            m = (Model<M>) old;
        } else {
            Model<M> m3 = m.maybeCopy();
            if (m3 != m) {
                m = m3;
            }
        }
        peekExprBuilder().setModel(m);
        m.pushExprBuilder((ExpressionBuilder<M>) peekExprBuilder());
        return m.root;
    }

    /**
     * Return the count of entities this query should return.
     */
    public int findCount() {
        return findCount(null);
    }

    public int findCount(Long tid) {
        checkDao("findCount");
        Select select = createSelect(tid);
        select.setGroupQuery();
        getSelectExpressions().clear();
        Aggregate a = new Aggregate(Aggregate.COUNT_ALL, null, select, false);
        getSelectExpressions().add(a);
        select.setExpressions(getSelectExpressions());
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(-1);
        reset();
        result.next();
        return result.currentRow()[0].getInt();
    }

    private ServerSession getSession(Long tid) {
        boolean autoCommit = false;
        ServerSession session;
        if (tid != null) {
            session = currentSessions.get(tid);
        } else {
            session = peekSession();
        }
        if (session == null) {
            session = modelTable.getSession();
            autoCommit = true;
        } else {
            autoCommit = false;
        }

        session.setAutoCommit(autoCommit);
        return session;
    }

    public long insert() {
        return insert(null);
    }

    public long insert(Long tid) {
        // TODO 是否允许通过 XXX.dao来insert记录?
        if (isDao()) {
            String name = this.getClass().getSimpleName();
            throw new UnsupportedOperationException("The insert operation is not allowed for " + name
                    + ".dao,  please use new " + name + "().insert() instead.");
        }
        ServerSession session = getSession(tid);
        Table dbTable = modelTable.getTable();
        Insert insert = new Insert(session);
        int size = nvPairs.size();
        Column[] columns = new Column[size];
        Expression[] expressions = new Expression[size];
        int i = 0;
        for (NVPair p : nvPairs) {
            columns[i] = dbTable.getColumn(p.name);
            expressions[i] = ValueExpression.get(p.value);
            i++;
        }
        insert.setColumns(columns);
        insert.addRow(expressions);
        insert.setTable(dbTable);
        insert.prepare();
        logger.info("execute sql: " + insert.getPlanSQL());
        insert.executeUpdate();
        long rowId = session.getLastRowKey();
        _rowid_.set(rowId);

        if (session.isAutoCommit()) {
            session.commit();
        }
        if (modelList != null) {
            for (Model<?> m : modelList) {
                m.insert(tid);
            }
        }
        reset();
        return rowId;
    }

    public int update() {
        return update(null);
    }

    public int update(Long tid) {
        ServerSession session = getSession(tid);
        Table dbTable = modelTable.getTable();
        Update update = new Update(session);
        TableFilter tableFilter = new TableFilter(session, dbTable, null, true, null);
        update.setTableFilter(tableFilter);
        checkWhereExpression(dbTable, "update");
        if (whereExpressionBuilder != null)
            update.setCondition(whereExpressionBuilder.getExpression());
        for (NVPair p : nvPairs) {
            update.setAssignment(dbTable.getColumn(p.name), ValueExpression.get(p.value));
        }
        update.prepare();
        reset();
        logger.info("execute sql: " + update.getPlanSQL());
        int count = update.executeUpdate();
        if (session.isAutoCommit()) {
            session.commit();
        }
        return count;
    }

    public int delete() {
        return delete(null);
    }

    public int delete(Long tid) {
        ServerSession session = getSession(tid);
        Table dbTable = modelTable.getTable();
        Delete delete = new Delete(session);
        TableFilter tableFilter = new TableFilter(session, dbTable, null, true, null);
        delete.setTableFilter(tableFilter);
        checkWhereExpression(dbTable, "delete");
        if (whereExpressionBuilder != null)
            delete.setCondition(whereExpressionBuilder.getExpression());
        delete.prepare();
        reset();
        logger.info("execute sql: " + delete.getPlanSQL());
        int count = delete.executeUpdate();
        if (session.isAutoCommit()) {
            session.commit();
        }
        return count;
    }

    private void checkWhereExpression(Table dbTable, String methodName) {
        if (whereExpressionBuilder == null || whereExpressionBuilder.getExpression() == null) {
            maybeCreateWhereExpression(dbTable);
            if (whereExpressionBuilder == null || whereExpressionBuilder.getExpression() == null) {
                checkDao(methodName);
            }
        } else {
            checkDao(methodName);
        }
    }

    private boolean isRootDao() {
        return modelType == ROOT_DAO;
    }

    @SuppressWarnings("unchecked")
    Model<T> maybeCopy() {
        if (isRootDao()) {
            return newInstance(modelTable.copy(), CHILD_DAO);
        } else {
            return this;
        }
    }

    private void maybeCreateWhereExpression(Table dbTable) {
        // 没有指定where条件时，如果存在ROWID，则用ROWID当where条件
        if (_rowid_.get() != 0) {
            peekExprBuilder().eq(Column.ROWID, _rowid_.get());
        } else {
            if (nvPairs == null)
                return;
            Index primaryKey = dbTable.findPrimaryKey();
            if (primaryKey != null) {
                for (Column c : primaryKey.getColumns()) {
                    // 如果主键由多个字段组成，当前面的字段没有指定时就算后面的指定了也不用它们来生成where条件
                    boolean found = false;
                    for (NVPair p : nvPairs) {
                        if (dbTable.getDatabase().equalsIdentifiers(p.name, c.getName())) {
                            peekExprBuilder().eq(p.name, p.value);
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        break;
                }
            }
        }
    }

    private ArrayStack<ExpressionBuilder<T>> getStack() {
        if (expressionBuilderStack == null) {
            expressionBuilderStack = new ArrayStack<ExpressionBuilder<T>>();
            expressionBuilderStack.push(getWhereExpressionBuilder());
        }
        return expressionBuilderStack;
    }

    TableFilter createTableFilter() {
        return new TableFilter(modelTable.getSession(), modelTable.getTable(), null, true, null);
    }

    private ArrayStack<TableFilter> getTableFilterStack() {
        if (tableFilterStack == null) {
            TableFilter tableFilter = createTableFilter();
            tableFilterStack = new ArrayStack<>();
            tableFilterStack.push(tableFilter);
        }
        return tableFilterStack;
    }

    /**
     * Push the expression builder onto the appropriate stack.
     */
    private T pushExprBuilder(ExpressionBuilder<T> builder) {
        getStack().push(builder);
        return root;
    }

    /**
     * Return the current expression builder that expressions should be added to.
     */
    ExpressionBuilder<T> peekExprBuilder() {
        return getStack().peek();
    }

    @Override
    public String toString() {
        JsonObject json = JsonObject.mapFrom(this);
        return json.encode();
    }

    /**
     * left parenthesis
     */
    public T lp() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.lp();
        }
        ExpressionBuilder<T> e = new ExpressionBuilder<>(this);
        pushExprBuilder(e);
        return root;
    }

    /**
     * right parenthesis
     */
    public T rp() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.rp();
        }
        ExpressionBuilder<T> right = getStack().pop();
        ExpressionBuilder<T> left = peekExprBuilder();
        left.junction(right);
        return root;
    }

    public T join(Model<?> m) {
        Model<T> m2 = maybeCopy();
        if (m2 != this) {
            return m2.join(m);
        }
        getTableFilterStack().push(m.createTableFilter());
        m.tableFilterStack = getTableFilterStack();
        return root;
    }

    public T on() {
        Model<T> m = maybeCopy();
        if (m != this) {
            return m.on();
        }
        ExpressionBuilder<T> e = new ExpressionBuilder<>(this);
        pushExprBuilder(e);
        return root;
    }

    public long beginTransaction() {
        checkDao("beginTransaction");
        Table dbTable = modelTable.getTable();
        ServerSession session = dbTable.getDatabase().createSession(modelTable.getSession().getUser());
        Transaction t = session.getTransaction();
        session.setAutoCommit(false);
        long tid = t.getTransactionId();
        currentSessions.put(tid, session);
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.get(hash);
        if (sessions == null) {
            sessions = new ArrayList<>();
            sessionMap.put(hash, sessions);
        }
        sessions.add(session);
        return tid;
    }

    public void commitTransaction() {
        checkDao("commitTransaction");
        Long tid = getAndRemoveLastTransaction();
        if (tid != null) {
            commitTransaction(tid.longValue());
        }
    }

    public void commitTransaction(long tid) {
        checkDao("commitTransaction");
        ServerSession s = currentSessions.remove(tid);
        if (s != null) {
            removeSession(tid);
            s.commit();
        }
    }

    public void rollbackTransaction() {
        checkDao("rollbackTransaction");
        Long tid = getAndRemoveLastTransaction();
        if (tid != null) {
            rollbackTransaction(tid.longValue());
        }
    }

    public void rollbackTransaction(long tid) {
        checkDao("rollbackTransaction");
        ServerSession s = currentSessions.remove(tid);
        if (s != null) {
            removeSession(tid);
            s.rollback();
        }
    }

    private ServerSession peekSession() {
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.get(hash);
        if (sessions != null && !sessions.isEmpty()) {
            return sessions.get(sessions.size() - 1);
        } else {
            return null;
        }
    }

    private void removeSession(long tid) {
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.get(hash);
        if (sessions != null && !sessions.isEmpty()) {
            int index = -1;
            for (ServerSession s : sessions) {
                index++;
                if (s.getTransaction().getTransactionId() == tid) {
                    break;
                }
            }
            if (index > -1) {
                sessions.remove(index);
            }
            if (sessions.isEmpty()) {
                sessionMap.remove(hash);
            }
        }
    }

    private Long getAndRemoveLastTransaction() {
        int hash = getCurrentThreadHashCode();
        List<ServerSession> sessions = sessionMap.remove(hash);
        Long tid = null;
        if (sessions != null && !sessions.isEmpty()) {
            ServerSession session = sessions.remove(sessions.size() - 1);
            tid = Long.valueOf(session.getTransaction().getTransactionId());
        }
        return tid;
    }

    private int getCurrentThreadHashCode() {
        return Thread.currentThread().hashCode();
    }

    /**
    * Stack based on ArrayList.
    *
    * @author rbygrave
    */
    static class ArrayStack<E> {

        private final List<E> list;

        /**
        * Creates an empty Stack with an initial size.
        */
        public ArrayStack(int size) {
            this.list = new ArrayList<>(size);
        }

        /**
        * Creates an empty Stack.
        */
        public ArrayStack() {
            this.list = new ArrayList<>();
        }

        @Override
        public String toString() {
            return list.toString();
        }

        /**
        * Pushes an item onto the top of this stack.
        */
        public void push(E item) {
            list.add(item);
        }

        /**
        * Removes the object at the top of this stack and returns that object as
        * the value of this function.
        */
        public E pop() {
            int len = list.size();
            if (len == 0) {
                throw new EmptyStackException();
            }
            return list.remove(len - 1);
        }

        private E peekZero(boolean retNull) {
            int len = list.size();
            if (len == 0) {
                if (retNull) {
                    return null;
                }
                throw new EmptyStackException();
            }
            return list.get(len - 1);
        }

        /**
        * Returns the object at the top of this stack without removing it.
        */
        public E peek() {
            return peekZero(false);
        }

        /**
        * Returns the object at the top of this stack without removing it.
        * If the stack is empty this returns null.
        */
        public E peekWithNull() {
            return peekZero(true);
        }

        /**
        * Tests if this stack is empty.
        */
        public boolean isEmpty() {
            return list.isEmpty();
        }

        public int size() {
            return list.size();
        }

        public E first() {
            int len = list.size();
            if (len == 0) {
                throw new EmptyStackException();
            }
            return list.get(0);
        }
    }
}
