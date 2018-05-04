/**
 * Provides type safe query criteria support for Ebean ORM queries.
 * <p>
 *   'Query beans' like QCustomer are generated using the <code>avaje-ebeanorm-typequery-generator</code>
 *   for each entity bean type and can then be used to build queries with type safe criteria.
 * </p>
 *
 * <h2>Example - usage of QCustomer</h2>
 * <pre>{@code
 *
 *    Date fiveDaysAgo = ...
 *
 *    List<Customer> customers =
 *        new QCustomer()
 *
 *          // name is a known property of type string so
 *          // it has relevant expressions such as like, startsWith etc
 *          .name.ilike("rob")
 *
 *          // status is a specific Enum type is equalTo() in() etc
 *          .status.equalTo(Customer.Status.GOOD)
 *
 *          // registered is a date type with after(), before() etc
 *          .registered.after(fiveDaysAgo)
 *
 *          // contacts is an associated bean containing specific
 *          // properties and in this case we use email which is a string type
 *          .contacts.email.endsWith("@foo.com")
 *
 *          .orderBy()
 *            .name.asc()
 *            .registered.desc()
 *          .findList();
 *
 * }</pre>
 */
package org.lealone.orm.typequery;