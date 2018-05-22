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

import java.util.List;

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
        SqlScript.createOrderItemTable(this);

        join();
    }

    void join() {
        Order o1 = new Order().orderId.set(2001).orderDate.set("2018-01-01");
        Order o2 = new Order().orderId.set(2002).orderDate.set("2018-01-01");
        Customer customer = new Customer().id.set(100).name.set("c1").phone.set(123);
        customer.addOrder(o1, o2).insert();

        Customer c = Customer.dao;
        Order o = Order.dao;

        customer = c.select(c.name, c.phone, o.orderId, o.orderDate).join(o).on().id.eq(o.customerId).where().id.eq(100)
                .findOne();

        List<Order> orderList = customer.getOrderList();
        assertEquals(2, orderList.size());
        assertTrue(customer == orderList.get(0).getCustomer());

        List<Customer> customerList = c.select(c.name, c.phone, o.orderId, o.orderDate).join(o).on().id.eq(o.customerId)
                .where().id.eq(100).findList();
        assertEquals(1, customerList.size());

        // customer = c.join(o).on().id.eq(o.customerId).where().id.eq(100).findOne();

        // SELECT c.name, c.phone, o.order_date, o.total FROM customer c JOIN order o ON c.id = o.customer_id
        // WHERE c.id = 100 or o.customer_id = 2
        // c.select(c.name, c.phone, o.orderDate, o.orderId).join(o).on().id.eq(o.customerId).where().id.eq(100).or()
        // .m(o).customerId.eq(2).m(c).findList();
    }
}
