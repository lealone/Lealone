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

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.index.Index;
import org.lealone.db.result.Result;
import org.lealone.db.table.Column;
import org.lealone.db.table.TableFilter;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.orm.typequery.PBaseNumber;
import org.lealone.orm.typequery.TQProperty;
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
 * Base root query bean.
 * <p>
 * With code generation for each entity bean type a query bean is created that extends this.
 * <p>
 * Provides common features for all root query beans
 * </p>
 * <p>
 * <h2>Example - QCustomer extends Query</h2>
 * <p>
 * These 'query beans' like QCustomer are generated using the <code>avaje-ebeanorm-typequery-generator</code>.
 * </p>
 * <pre>{@code
 *
 *   public class QCustomer extends Query<Customer,QCustomer> {
 *
 *     // properties
 *     public PLong<QCustomer> id;
 *
 *     public PString<QCustomer> name;
 *     ...
 *
 * }</pre>
 * <p>
 * <h2>Example - usage of QCustomer</h2>
 * <pre>{@code
 *
 *    Date fiveDaysAgo = ...
 *
 *    List<Customer> customers =
 *        new QCustomer()
 *          .name.ilike("rob")
 *          .status.equalTo(Customer.Status.GOOD)
 *          .registered.after(fiveDaysAgo)
 *          .contacts.email.endsWith("@foo.com")
 *          .orderBy()
 *            .name.asc()
 *            .registered.desc()
 *          .findList();
 *
 * }</pre>
 * <p>
 * <h2>Resulting SQL where</h2>
 * <p>
 * <pre>{@code sql
 *
 *     where lower(t0.name) like ?  and t0.status = ?  and t0.registered > ?  and u1.email like ?
 *     order by t0.name, t0.registered desc;
 *
 *     --bind(rob,GOOD,Mon Jul 27 12:05:37 NZST 2015,%@foo.com)
 * }</pre>
 *
 * @param <T> the entity bean type (normal entity bean type e.g. Customer) 
 */
@SuppressWarnings("rawtypes")
public abstract class Query<T> {

    private static final Logger logger = LoggerFactory.getLogger(Query.class);

    TQProperty[] tqProperties;

    @SuppressWarnings("unchecked")
    public final PRowId _rowid_ = new PRowId(Column.ROWID, (T) this);

    public class PRowId extends PBaseNumber<T, Long> {

        private long value;

        /**
         * Construct with a property name and root instance.
         *
         * @param name property name
         * @param root the root query bean instance
         */
        public PRowId(String name, T root) {
            super(name, root);
        }

        /**
         * Construct with additional path prefix.
         */
        public PRowId(String name, T root, String prefix) {
            super(name, root, prefix);
        }

        // 不需要通过外部设置
        T set(long value) {
            if (!areEqual(this.value, value)) {
                this.value = value;
                changed = true;
                if (isReady()) {
                    expr().set(name, ValueLong.get(value));
                }
            }
            return root;
        }

        @Override
        public final T deserialize(HashMap<String, Value> map) {
            Value v = map.get(name);
            if (v != null) {
                value = v.getLong();
            }
            return root;
        }

        public final long get() {
            return value;
        }

    }

    public static class NVPair {
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

    private final HashSet<NVPair> nvPairs = new HashSet<>();

    protected Table table;
    protected final String tableName;

    private final ArrayList<Expression> selectExpressions = new ArrayList<>();

    /**
     * The root query bean instance. Used to provide fluid query construction.
     */
    private T root;
    private DefaultExpressionList<T> whereExpression;

    /**
    * The underlying expression lists held as a stack. Pushed and popped based on and/or (conjunction/disjunction).
    */
    private ArrayStack<ExpressionList<T>> whereStack;

    public Query(Table table, String tableName) {
        this.table = table;
        this.tableName = tableName;
        reset();
    }

    protected void setTQProperties(TQProperty[] tqProperties) {
        this.tqProperties = tqProperties;
    }

    void addNVPair(String name, Value value) {
        nvPairs.add(new NVPair(name, value));
    }

    /**
     * Sets the root query bean instance. Used to provide fluid query construction.
     */
    protected void setRoot(T root) {
        this.root = root;
    }

    private void reset() {
        whereStack = null;
        selectExpressions.clear();
        whereExpression = new DefaultExpressionList<T>(this, table);
        nvPairs.clear();
    }

    /**
     * Tune the query by specifying the properties to be loaded on the
     * 'main' root level entity bean (aka partial object).
     * <pre>{@code
     *
     *   // alias for the customer properties in select()
     *   QCustomer cust = QCustomer.alias();
     *
     *   // alias for the contact properties in contacts.fetch()
     *   QContact contact = QContact.alias();
     *
     *   List<Customer> customers =
     *     new QCustomer()
     *       // tune query
     *       .select(cust.id, cust.name)
     *       .contacts.fetch(contact.firstName, contact.lastName, contact.email)
     *
     *       // predicates
     *       .id.greaterThan(1)
     *       .findList();
     *
     * }</pre>
     *
     * @param properties the list of properties to fetch
     */
    @SafeVarargs
    public final T select(TQProperty<T>... properties) {
        org.lealone.db.table.Table dbTable = table.getDbTable();
        for (TQProperty<T> p : properties) {
            ExpressionColumn c = new ExpressionColumn(dbTable.getDatabase(), dbTable.getSchema().getName(),
                    dbTable.getName(), p.propertyName());
            // c = new ExpressionColumn(dbTable.getDatabase(), dbTable.getColumn(p.propertyName()));
            selectExpressions.add(c);
        }
        return root;
    }

    /**
     * Marker that can be used to indicate that the order by clause is defined after this.
     * <p>
     * order() and orderBy() are synonyms and both exist for historic reasons.
     * </p>
     * <p>
     * <h2>Example: order by customer name, order date</h2>
     * <pre>{@code
     *   List<Order> orders =
     *          new QOrder()
     *            .customer.name.ilike("rob")
     *            .orderBy()
     *              .customer.name.asc()
     *              .orderDate.asc()
     *            .findList();
     *
     * }</pre>
     */
    public T orderBy() {
        // Yes this does not actually do anything! We include it because style wise it makes
        // the query nicer to read and suggests that order by definitions are added after this
        return root;
    }

    /**
     * Marker that can be used to indicate that the order by clause is defined after this.
     * <p>
     * order() and orderBy() are synonyms and both exist for historic reasons.
     * </p>
     * <p>
     * <h2>Example: order by customer name, order date</h2>
     * <pre>{@code
     *   List<Order> orders =
     *          new QOrder()
     *            .customer.name.ilike("rob")
     *            .order()
     *              .customer.name.asc()
     *              .orderDate.asc()
     *            .findList();
     *
     * }</pre>
     */
    public T order() {
        // Yes this does not actually do anything! We include it because style wise it makes
        // the query nicer to read and suggests that order by definitions are added after this
        return root;
    }

    /**
     * Begin a list of expressions added by 'OR'.
     * <p>
     * Use endOr() or endJunction() to stop added to OR and 'pop' to the parent expression list.
     * </p>
     * <p>
     * <h2>Example</h2>
     * <p>
     * This example uses an 'OR' expression list with an inner 'AND' expression list.
     * </p>
     * <pre>{@code
     *
     *    List<Customer> customers =
     *          new QCustomer()
     *            .status.equalTo(Customer.Status.GOOD)
     *            .or()
     *              .id.greaterThan(1000)
     *              .and()
     *                .name.startsWith("super")
     *                .registered.after(fiveDaysAgo)
     *              .endAnd()
     *            .endOr()
     *            .orderBy().id.desc()
     *            .findList();
     *
     * }</pre>
     * <h2>Resulting SQL where clause</h2>
     * <pre>{@code sql
     *
     *    where t0.status = ?  and (t0.id > ?  or (t0.name like ?  and t0.registered > ? ) )
     *    order by t0.id desc;
     *
     *    --bind(GOOD,1000,super%,Wed Jul 22 00:00:00 NZST 2015)
     *
     * }</pre>
     */
    public T or() {
        pushExprList(peekExprList().or());
        return root;
    }

    /**
     * Begin a list of expressions added by 'AND'.
     * <p>
     * Use endAnd() or endJunction() to stop added to AND and 'pop' to the parent expression list.
     * </p>
     * <p>
     * Note that typically the AND expression is only used inside an outer 'OR' expression.
     * This is because the top level expression list defaults to an 'AND' expression list.
     * </p>
     * <h2>Example</h2>
     * <p>
     * This example uses an 'OR' expression list with an inner 'AND' expression list.
     * </p>
     * <pre>{@code
     *
     *    List<Customer> customers =
     *          new QCustomer()
     *            .status.equalTo(Customer.Status.GOOD)
     *            .or() // OUTER 'OR'
     *              .id.greaterThan(1000)
     *              .and()  // NESTED 'AND' expression list
     *                .name.startsWith("super")
     *                .registered.after(fiveDaysAgo)
     *                .endAnd()
     *              .endOr()
     *            .orderBy().id.desc()
     *            .findList();
     *
     * }</pre>
     * <h2>Resulting SQL where clause</h2>
     * <pre>{@code sql
     *
     *    where t0.status = ?  and (t0.id > ?  or (t0.name like ?  and t0.registered > ? ) )
     *    order by t0.id desc;
     *
     *    --bind(GOOD,1000,super%,Wed Jul 22 00:00:00 NZST 2015)
     *
     * }</pre>
     */
    public T and() {
        pushExprList(peekExprList().and());
        return root;
    }

    /**
     * Begin a list of expressions added by NOT.
     * <p>
     * Use endNot() or endJunction() to stop added to NOT and 'pop' to the parent expression list.
     * </p>
     */
    public T not() {
        pushExprList(peekExprList().not());
        return root;
    }

    /**
     * End a list of expressions added by 'OR'.
     */
    public T endJunction() {
        whereStack.pop();
        return root;
    }

    /**
     * End OR junction - synonym for endJunction().
     */
    public T endOr() {
        return endJunction();
    }

    /**
     * End AND junction - synonym for endJunction().
     */
    public T endAnd() {
        return endJunction();
    }

    /**
     * End NOT junction - synonym for endJunction().
     */
    public T endNot() {
        return endJunction();
    }

    /**
     * Push the expression list onto the appropriate stack.
     */
    private T pushExprList(ExpressionList<T> list) {
        whereStack.push(list);
        return root;
    }

    /**
     * Add expression after this to the WHERE expression list.
     * <p>
     * For queries against the normal database (not the doc store) this has no effect.
     * </p>
     * <p>
     * This is intended for use with Document Store / ElasticSearch where expressions can be put into either
     * the "query" section or the "filter" section of the query. Full text expressions like MATCH are in the
     * "query" section but many expression can be in either - expressions after the where() are put into the
     * "filter" section which means that they don't add to the relevance and are also cache-able.
     * </p>
     */
    public T where() {
        return root;
    }

    /**
     * Execute the query returning either a single bean or null (if no matching
     * bean is found).
     * <p>
     * If more than 1 row is found for this query then a PersistenceException is
     * thrown.
     * </p>
     * <p>
     * This is useful when your predicates dictate that your query should only
     * return 0 or 1 results.
     * </p>
     * <p>
     * <pre>{@code
     *
     * // assuming the sku of products is unique...
     * Product product =
     *     new QProduct()
     *         .sku.equalTo("aa113")
     *         .findUnique();
     * ...
     * }</pre>
     * <p>
     * <p>
     * It is also useful with finding objects by their id when you want to specify
     * further join information to optimise the query.
     * </p>
     * <p>
     * <pre>{@code
     *
     * // Fetch order 42 and additionally fetch join its order details...
     * Order order =
     *     new QOrder()
     *         .fetch("details") // eagerly load the order details
     *         .id.equalTo(42)
     *         .findOne();
     *
     * // the order details were eagerly loaded
     * List<OrderDetail> details = order.getDetails();
     * ...
     * }</pre>
     */
    public T findOne() {
        getTable();
        Select select = new Select(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        select.addTableFilter(tableFilter, true);
        if (selectExpressions.isEmpty()) {
            selectExpressions.add(new Wildcard(null, null));
        }
        selectExpressions.add(createRowIdExpressionColumn(table.getDbTable())); // 总是获取rowid
        select.setExpressions(selectExpressions);
        select.addCondition(whereExpression.expression);
        select.setLimit(ValueExpression.get(ValueInt.get(1)));
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(1);
        result.next();
        reset();
        return deserialize(result);
    }

    private ExpressionColumn createRowIdExpressionColumn(org.lealone.db.table.Table dbTable) {
        return new ExpressionColumn(dbTable.getDatabase(), dbTable.getSchema().getName(), dbTable.getName(),
                Column.ROWID);
    }

    @SuppressWarnings("unchecked")
    private T deserialize(Result result) {
        Value[] row = result.currentRow();
        if (row == null)
            return null;

        int len = row.length;
        HashMap<String, Value> map = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
            map.put(result.getColumnName(i), row[i]);
        }

        Query q = newInstance(table);
        if (q != null) {
            for (TQProperty p : q.tqProperties) {
                p.deserialize(map);
            }
            q._rowid_.deserialize(map);
        }
        return (T) q;
    }

    protected Query newInstance(Table t) {
        return null;
    }

    protected void deserialize(JsonNode node) {
        for (TQProperty p : tqProperties) {
            p.deserialize(node);
        }
    }

    /**
     * Execute the query returning the list of objects.
     * <p>
     * This query will execute against the EbeanServer that was used to create it.
     * </p>
     * <p>
     * <pre>{@code
     *
     * List<Customer> customers =
     *     new QCustomer()
     *       .name.ilike("rob%")
     *       .findList();
     *
     * }</pre>
     *
     * @see EbeanServer#findList(Query, Transaction)
     */
    public List<T> findList() {
        getTable();
        Select select = new Select(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        select.addTableFilter(tableFilter, true);
        if (selectExpressions.isEmpty()) {
            selectExpressions.add(new Wildcard(null, null));
        }
        selectExpressions.add(createRowIdExpressionColumn(table.getDbTable())); // 总是获取rowid
        select.setExpressions(selectExpressions);
        select.addCondition(whereExpression.expression);
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        Result result = select.executeQuery(-1);
        reset();
        ArrayList<T> list = new ArrayList<>(result.getRowCount());
        while (result.next()) {
            list.add(deserialize(result));
        }
        return list;
    }

    /**
     * Return the count of entities this query should return.
     * <p>
     * This is the number of 'top level' or 'root level' entities.
     * </p>
     */
    public int findCount() {
        getTable();
        Select select = new Select(table.getSession());
        select.setGroupQuery();
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        select.addTableFilter(tableFilter, true);
        selectExpressions.clear();
        Aggregate a = new Aggregate(Aggregate.COUNT_ALL, null, select, false);
        selectExpressions.add(a);
        select.setExpressions(selectExpressions);
        select.addCondition(whereExpression.expression);
        select.init();
        select.prepare();
        logger.info("execute sql: " + select.getPlanSQL());
        org.lealone.db.result.Result result = select.executeQuery(-1);
        reset();
        result.next();
        return result.currentRow()[0].getInt();
    }

    /**
     * Execute as a delete query deleting the 'root level' beans that match the predicates
     * in the query.
     * <p>
     * Note that if the query includes joins then the generated delete statement may not be
     * optimal depending on the database platform.
     * </p>
     *
     * @return the number of beans/rows that were deleted.
     */
    public int delete() {
        getTable();
        org.lealone.db.table.Table dbTable = table.getDbTable();
        Delete delete = new Delete(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), dbTable, null, true, null);
        delete.setTableFilter(tableFilter);
        if (whereExpression.expression == null) {
            maybeCreateWhereExpression(dbTable);
        }
        delete.setCondition(whereExpression.expression);
        delete.prepare();
        reset();
        logger.info("execute sql: " + delete.getPlanSQL());
        int count = delete.executeUpdate();
        commit();
        return count;
    }

    public int update() {
        getTable();
        org.lealone.db.table.Table dbTable = table.getDbTable();
        Update update = new Update(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), dbTable, null, true, null);
        update.setTableFilter(tableFilter);
        if (whereExpression.expression == null) {
            maybeCreateWhereExpression(dbTable);
        }
        update.setCondition(whereExpression.expression);
        for (NVPair p : nvPairs) {
            update.setAssignment(dbTable.getColumn(p.name), ValueExpression.get(p.value));
        }
        update.prepare();
        reset();
        logger.info("execute sql: " + update.getPlanSQL());
        int count = update.executeUpdate();
        commit();
        return count;
    }

    private void maybeCreateWhereExpression(org.lealone.db.table.Table dbTable) {
        // 没有指定where条件时，如果存在ROWID，则用ROWID当where条件
        if (_rowid_.get() != 0) {
            peekExprList().eq(Column.ROWID, _rowid_.get());
        } else {
            Index primaryKey = dbTable.findPrimaryKey();
            if (primaryKey != null) {
                for (Column c : primaryKey.getColumns()) {
                    // 如果主键由多个字段组成，当前面的字段没有指定时就算后面的指定了也不用它们来生成where条件
                    boolean found = false;
                    for (NVPair p : nvPairs) {
                        if (dbTable.getDatabase().equalsIdentifiers(p.name, c.getName())) {
                            peekExprList().eq(p.name, p.value);
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

    // 可能是延迟关联到Table
    Table getTable() {
        if (table == null) {
            String url = System.getProperty("lealone.jdbc.url");
            if (url == null) {
                throw new RuntimeException("'lealone.jdbc.url' must be set");
            }
            table = new Table(url, tableName);
        }
        return table;
    }

    private void commit() {
        table.getSession().commit();
    }

    public long insert() {
        getTable();
        org.lealone.db.table.Table dbTable = table.getDbTable();
        Insert insert = new Insert(table.getSession());
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
        long rowId = table.getSession().getLastRowKey();
        _rowid_.set(rowId);
        commit();
        reset();
        return rowId;
    }

    /**
    * Return the expression list that has been built for this query.
    */
    public ExpressionList<T> getExpressionList() {
        return whereExpression;
    }

    /**
     * Return the current expression list that expressions should be added to.
     */
    public ExpressionList<T> peekExprList() {
        if (whereStack == null) {
            whereStack = new ArrayStack<ExpressionList<T>>();
            whereStack.push(whereExpression);
        }
        // return the current expression list
        return whereStack.peek();
    }

    public boolean databaseToUpper() {
        if (table == null)
            return false;
        return table.getSession().getDatabase().getSettings().databaseToUpper;
    }

    public boolean isReady() {
        return table != null;
    }

    @Override
    public String toString() {
        JsonObject json = JsonObject.mapFrom(this);
        return json.encode();
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

        public boolean contains(E o) {
            return list.contains(o);
        }
    }
}
