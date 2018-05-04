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
import java.util.List;

import org.lealone.db.table.TableFilter;
import org.lealone.db.value.ValueInt;
import org.lealone.orm.typequery.TQProperty;
import org.lealone.sql.dml.Delete;
import org.lealone.sql.dml.Select;
import org.lealone.sql.dml.Update;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Wildcard;
import org.lealone.sql.expression.aggregate.Aggregate;
import org.lealone.transaction.Transaction;

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
 * @param <R> the specific root query bean type (e.g. QCustomer)
 */
public abstract class Query<T, R> {

    private final Table table;

    private final ArrayList<Expression> selectExpressions = new ArrayList<>();

    /**
     * The root query bean instance. Used to provide fluid query construction.
     */
    private R root;
    private DefaultExpressionList<T> whereExpression;

    /**
    * The underlying expression lists held as a stack. Pushed and popped based on and/or (conjunction/disjunction).
    */
    private ArrayStack<ExpressionList<T>> whereStack;

    public Query(Table table) {
        this.table = table;
        reset();
    }

    /**
     * Sets the root query bean instance. Used to provide fluid query construction.
     */
    protected void setRoot(R root) {
        this.root = root;
    }

    private void reset() {
        whereStack = null;
        selectExpressions.clear();
        whereExpression = new DefaultExpressionList<T>(table);
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
    public final R select(TQProperty<R>... properties) {
        org.lealone.db.table.Table dbTable = table.getDbTable();
        for (TQProperty<R> p : properties) {
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
    public R orderBy() {
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
    public R order() {
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
    public R or() {
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
    public R and() {
        pushExprList(peekExprList().and());
        return root;
    }

    /**
     * Begin a list of expressions added by NOT.
     * <p>
     * Use endNot() or endJunction() to stop added to NOT and 'pop' to the parent expression list.
     * </p>
     */
    public R not() {
        pushExprList(peekExprList().not());
        return root;
    }

    /**
     * End a list of expressions added by 'OR'.
     */
    public R endJunction() {
        whereStack.pop();
        return root;
    }

    /**
     * End OR junction - synonym for endJunction().
     */
    public R endOr() {
        return endJunction();
    }

    /**
     * End AND junction - synonym for endJunction().
     */
    public R endAnd() {
        return endJunction();
    }

    /**
     * End NOT junction - synonym for endJunction().
     */
    public R endNot() {
        return endJunction();
    }

    /**
     * Push the expression list onto the appropriate stack.
     */
    private R pushExprList(ExpressionList<T> list) {
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
    public R where() {
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
        Select select = new Select(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        select.addTableFilter(tableFilter, true);
        if (selectExpressions.isEmpty()) {
            selectExpressions.add(new Wildcard(null, null));
        }
        select.setExpressions(selectExpressions);
        select.addCondition(whereExpression.expression);
        select.setLimit(ValueExpression.get(ValueInt.get(1)));
        select.init();
        select.prepare();
        select.executeQuery(1);
        reset();
        return null; // TODO
        // return query.findOne();
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
        Select select = new Select(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        select.addTableFilter(tableFilter, true);
        if (selectExpressions.isEmpty()) {
            selectExpressions.add(new Wildcard(null, null));
        }
        select.setExpressions(selectExpressions);
        select.addCondition(whereExpression.expression);
        select.init();
        select.prepare();
        select.executeQuery(-1);
        reset();
        return new ArrayList<>(); // TODO
    }

    /**
     * Return the count of entities this query should return.
     * <p>
     * This is the number of 'top level' or 'root level' entities.
     * </p>
     */
    public int findCount() {
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
        Delete delete = new Delete(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        delete.setTableFilter(tableFilter);
        delete.setCondition(whereExpression.expression);
        delete.prepare();
        reset();
        return delete.executeUpdate();
    }

    public int update() {
        Update update = new Update(table.getSession());
        TableFilter tableFilter = new TableFilter(table.getSession(), table.getDbTable(), null, true, null);
        update.setTableFilter(tableFilter);
        update.setCondition(whereExpression.expression);
        update.prepare();
        reset();
        return update.executeUpdate();
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
        return table.getSession().getDatabase().getSettings().databaseToUpper;
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
