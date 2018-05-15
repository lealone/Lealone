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
package org.lealone.test.orm;

import org.lealone.test.SqlScript;
import org.lealone.test.UnitTestBase;
import org.lealone.test.generated.model.Customer;
import org.lealone.test.generated.model.Order;

public class OrmJoinTest extends UnitTestBase {

    public static void main(String[] args) {
        new OrmJoinTest().runTest();
    }

    @Override
    public void test() {
        SqlScript.createCustomerTable(this);
        SqlScript.createProductTable(this);
        SqlScript.createOrderTable(this);

        join();
    }

    void join() {
        Customer c = Customer.dao;
        Order o = Order.dao;

        // SELECT c.name, c.phone, o.order_date, o.total FROM customer c JOIN order o ON c.id = o.customer_id
        // WHERE c.id = 1 or o.customer_id = 2
        c.select(c.name, c.phone, o.orderDate, o.total).join(o).on().id.eq(o.customerId).where().id.eq(1)
                .or(o).customerId.eq(2).findList();
    }
}
