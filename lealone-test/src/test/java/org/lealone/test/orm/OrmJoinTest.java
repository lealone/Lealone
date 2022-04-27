/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import java.util.List;

import org.junit.Test;
import org.lealone.test.orm.generated.Customer;
import org.lealone.test.orm.generated.CustomerAddress;
import org.lealone.test.orm.generated.Order;

public class OrmJoinTest extends OrmTestBase {

    @Test
    public void run() {
        // SqlScript.createCustomerTable(this);
        // SqlScript.createCustomerAddressTable(this);
        // SqlScript.createProductTable(this);
        // SqlScript.createOrderTable(this);
        // SqlScript.createOrderItemTable(this);

        join();
    }

    void join() {
        Order o1 = new Order().orderId.set(2001).orderDate.set("2018-01-01");
        Order o2 = new Order().orderId.set(2002).orderDate.set("2018-01-01");
        Customer customer = new Customer().id.set(100).name.set("c1").phone.set(123);
        customer.addOrder(o1, o2).insert();

        Order o30 = new Order().orderId.set(2003).orderDate.set("2018-01-01");
        Order o40 = new Order().orderId.set(2004).orderDate.set("2018-01-01");
        customer = new Customer().id.set(600).name.set("c2").phone.set(456);
        customer.addOrder(o30, o40).insert();

        Customer c = Customer.dao;
        Order o = Order.dao;

        List<Customer> customerList = c.join(o).on().id.eq(o.customerId).orderBy().id.desc().findList();
        assertEquals(2, customerList.size());
        List<Order> orderList = customerList.get(0).getOrderList();
        assertEquals(2, orderList.size());

        customer = c.select(c.name, c.phone, o.orderId, o.orderDate).join(o).on().id.eq(o.customerId).where().id.eq(100)
                .findOne();
        orderList = customer.getOrderList();
        assertEquals(2, orderList.size());
        assertTrue(customer == orderList.get(0).getCustomer());

        customerList = c.select(c.id, c.name, c.phone, o.orderId, o.orderDate).join(o).on().id.eq(o.customerId)
                .where().id.eq(100).findList();
        assertEquals(1, customerList.size());

        customer = c.join(o).on().id.eq(o.customerId).where().id.eq(100).findOne();

        try {
            Order o3 = new Order().orderId.set(2003).orderDate.set("2018-01-02"); // orderId 重复了
            new Customer().id.set(200).name.set("c2").phone.set(123).addOrder(o3).insert();
            fail();
        } catch (Exception e) {
        }

        Order o3 = new Order().orderId.set(2022).orderDate.set("2018-01-02"); // orderId 重复了
        new Customer().id.set(200).name.set("c2").phone.set(123).addOrder(o3).insert();

        // SELECT c.name, c.phone, o.order_id, o.order_date FROM customer c JOIN order o ON c.id = o.customer_id
        // WHERE c.id = 100 or o.customer_id = 200
        customerList = c.select(c.name, c.phone, o.orderId, o.orderDate).join(o).on().id.eq(o.customerId).where().id
                .eq(100).or().m(o).customerId.eq(200).m(c).findList();
        assertEquals(2, customerList.size());
        assertEquals(2, customerList.get(0).getOrderList().size());
        assertEquals(1, customerList.get(1).getOrderList().size());

        Customer customer3 = new Customer().id.set(300).name.set("c3");
        CustomerAddress a1 = new CustomerAddress().city.set("c1").street.set("s1");
        CustomerAddress a2 = new CustomerAddress().city.set("c2").street.set("s2");
        Order o4 = new Order().orderId.set(2014).orderDate.set("2018-01-03");
        Order o5 = new Order().orderId.set(2015).orderDate.set("2018-01-03");
        customer3.addOrder(o4, o5).addCustomerAddress(a1, a2).insert();

        c = Customer.dao;
        o = Order.dao;
        CustomerAddress a = CustomerAddress.dao;
        // customerList = c.join(o).join(a).on().id.eq(o.customerId).and().id.eq(a.customerId).where().id.eq(300)
        // .findList();

        // customerList =
        // c.join(o).on().id.eq(o.customerId).join(a).on().m(o).customerId.eq(a.customerId).where().m(c).id
        // .eq(300).findList();

        customerList = c.join(o).on().id.eq(o.customerId).join(a).on().id.eq(a.customerId).where().id.eq(300)
                .findList();

        assertEquals(1, customerList.size());
        assertEquals(2, customerList.get(0).getOrderList().size());
        assertEquals(2, customerList.get(0).getCustomerAddressList().size());

        String sql = "SELECT count(*) FROM customer c JOIN `order` o JOIN customer_address a" //
                + " ON c.id = o.customer_id AND c.id = a.customer_id" //
                + " WHERE c.id = 300";

        sql = "SELECT count(*) FROM customer c JOIN `order` o ON c.id = o.customer_id"
                + " JOIN customer_address a ON a.customer_id = o.customer_id" //
                + " WHERE c.id = 300";

        sql = "SELECT count(*) FROM customer c JOIN `order` o ON c.id = o.customer_id"
                + " JOIN customer_address a ON c.id = a.customer_id" //
                + " WHERE c.id = 300";
        System.out.println();
        System.out.println("count = " + count(sql));
        explain(sql);
    }
}
