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

import java.util.Collection;

import org.lealone.db.value.Value;

public interface ExpressionList<T> {

    /**
     * Equal To - property is equal to a given value.
     */
    ExpressionList<T> set(String propertyName, Value value);

    /**
     * Equal To - property is equal to a given value.
     */
    ExpressionList<T> eq(String propertyName, Object value);

    /**
     * Not Equal To - property not equal to the given value.
     */
    ExpressionList<T> ne(String propertyName, Object value);

    /**
     * Case Insensitive Equal To - property equal to the given value (typically
     * using a lower() function to make it case insensitive).
     */
    ExpressionList<T> ieq(String propertyName, String value);

    /**
     * Between - property between the two given values.
     */
    ExpressionList<T> between(String propertyName, Object value1, Object value2);

    /**
     * Between - value between the two properties.
     */
    ExpressionList<T> betweenProperties(String lowProperty, String highProperty, Object value);

    /**
     * Greater Than - property greater than the given value.
     */
    ExpressionList<T> gt(String propertyName, Object value);

    /**
     * Greater Than or Equal to - property greater than or equal to the given
     * value.
     */
    ExpressionList<T> ge(String propertyName, Object value);

    /**
     * Less Than - property less than the given value.
     */
    ExpressionList<T> lt(String propertyName, Object value);

    /**
     * Less Than or Equal to - property less than or equal to the given value.
     */
    ExpressionList<T> le(String propertyName, Object value);

    /**
     * Is Null - property is null.
     */
    ExpressionList<T> isNull(String propertyName);

    /**
     * Is Not Null - property is not null.
     */
    ExpressionList<T> isNotNull(String propertyName);

    /**
     * Array property contains entries with the given values.
     */
    ExpressionList<T> arrayContains(String propertyName, Object... values);

    /**
     * Array does not contain the given values.
     * <p>
     * Array support is effectively limited to Postgres at this time.
     * </p>
     */
    ExpressionList<T> arrayNotContains(String propertyName, Object... values);

    /**
     * Array is empty - for the given array property.
     * <p>
     * Array support is effectively limited to Postgres at this time.
     * </p>
     */
    ExpressionList<T> arrayIsEmpty(String propertyName);

    /**
     * Array is not empty - for the given array property.
     * <p>
     * Array support is effectively limited to Postgres at this time.
     * </p>
     */
    ExpressionList<T> arrayIsNotEmpty(String propertyName);

    /**
     * In - property has a value in the array of values.
     */
    ExpressionList<T> in(String propertyName, Object... values);

    /**
     * In - property has a value in the collection of values.
     */
    ExpressionList<T> in(String propertyName, Collection<?> values);

    /**
     * Not In - property has a value in the array of values.
     */
    ExpressionList<T> notIn(String propertyName, Object... values);

    /**
     * Not In - property has a value in the collection of values.
     */
    ExpressionList<T> notIn(String propertyName, Collection<?> values);

    /**
     * Like - property like value where the value contains the SQL wild card
     * characters % (percentage) and _ (underscore).
     */
    ExpressionList<T> like(String propertyName, String value);

    /**
     * Case insensitive Like - property like value where the value contains the
     * SQL wild card characters % (percentage) and _ (underscore). Typically uses
     * a lower() function to make the expression case insensitive.
     */
    ExpressionList<T> ilike(String propertyName, String value);

    /**
     * Starts With - property like value%.
     */
    ExpressionList<T> startsWith(String propertyName, String value);

    /**
     * Case insensitive Starts With - property like value%. Typically uses a
     * lower() function to make the expression case insensitive.
     */
    ExpressionList<T> istartsWith(String propertyName, String value);

    /**
     * Ends With - property like %value.
     */
    ExpressionList<T> endsWith(String propertyName, String value);

    /**
     * Case insensitive Ends With - property like %value. Typically uses a lower()
     * function to make the expression case insensitive.
     */
    ExpressionList<T> iendsWith(String propertyName, String value);

    /**
     * Contains - property like %value%.
     */
    ExpressionList<T> contains(String propertyName, String value);

    /**
     * Case insensitive Contains - property like %value%. Typically uses a lower()
     * function to make the expression case insensitive.
     */
    ExpressionList<T> icontains(String propertyName, String value);

    /**
    * Add a match expression.
    *
    * @param propertyName The property name for the match
    * @param search       The search value
    */
    ExpressionList<T> match(String propertyName, String search);

    /**
     * Start a list of expressions that will be joined by AND's
     * returning the expression list the expressions are added to.
     * <p>
     * This is exactly the same as conjunction();
     * </p>
     * <p>
     * Use endAnd() or endJunction() to end the AND junction.
     * </p>
     * <p>
     * Note that a where() clause defaults to an AND junction so
     * typically you only explicitly need to use the and() junction
     * when it is nested inside an or() or not() junction.
     * </p>
     * <p>
     * <pre>{@code
     *
     *  // Example: Nested and()
     *
     *  Ebean.find(Customer.class)
     *    .where()
     *    .or()
     *      .and() // nested and
     *        .startsWith("name", "r")
     *        .eq("anniversary", onAfter)
     *        .endAnd()
     *      .and()
     *        .eq("status", Customer.Status.ACTIVE)
     *        .gt("id", 0)
     *        .endAnd()
     *      .orderBy().asc("name")
     *      .findList();
     * }</pre>
     */
    ExpressionList<T> and();

    /**
     * Return a list of expressions that will be joined by OR's.
     * This is exactly the same as disjunction();
     * <p>
     * <p>
     * Use endOr() or endJunction() to end the OR junction.
     * </p>
     * <p>
     * <pre>{@code
     *
     *  // Example: Use or() to join
     *  // two nested and() expressions
     *
     *  Ebean.find(Customer.class)
     *    .where()
     *    .or()
     *      .and()
     *        .startsWith("name", "r")
     *        .eq("anniversary", onAfter)
     *        .endAnd()
     *      .and()
     *        .eq("status", Customer.Status.ACTIVE)
     *        .gt("id", 0)
     *        .endAnd()
     *      .orderBy().asc("name")
     *      .findList();
     *
     * }</pre>
     */
    ExpressionList<T> or();

    /**
     * Return a list of expressions that will be wrapped by NOT.
     * <p>
     * Use endNot() or endJunction() to end expressions being added to the
     * NOT expression list.
     * </p>
     * <p>
     * <pre>@{code
     *
     *    .where()
     *      .not()
     *        .gt("id", 1)
     *        .eq("anniversary", onAfter)
     *        .endNot()
     *
     * }</pre>
     * <p>
     * <pre>@{code
     *
     * // Example: nested not()
     *
     * Ebean.find(Customer.class)
     *   .where()
     *     .eq("status", Customer.Status.ACTIVE)
     *     .not()
     *       .gt("id", 1)
     *       .eq("anniversary", onAfter)
     *       .endNot()
     *     .orderBy()
     *       .asc("name")
     *     .findList();
     *
     * }</pre>
     */
    ExpressionList<T> not();

    /**
     * Return the OrderBy so that you can append an ascending or descending
     * property to the order by clause.
     * <p>
     * This will never return a null. If no order by clause exists then an 'empty'
     * OrderBy object is returned.
     * </p>
     */
    OrderBy<?> orderBy();
}
