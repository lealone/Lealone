/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.orm;

import org.junit.Test;

import com.lealone.test.orm.generated.Customer;

public class OrmNullTest extends OrmTestBase {
    @Test
    public void run() {
        // insert
        new Customer().id.set(1001).name.set("rob").phone.set(12345678).insert();

        Customer c = Customer.dao.where().notes.isNull().findOne();
        assertNotNull(c);

        c.notes.set("abc").update();

        c = Customer.dao.where().notes.isNotNull().findOne();
        assertNotNull(c);

        c.notes.set(null).update();
        c = Customer.dao.where().notes.isNull().findOne();
        assertNotNull(c);

        c.notes.set("abc").update();
        c = Customer.dao.where().notes.isNotNull().findOne();
        assertNotNull(c);

        Customer.dao.notes.set(null).update();
        c = Customer.dao.where().notes.isNotNull().findOne();
        assertNull(c);
    }
}
