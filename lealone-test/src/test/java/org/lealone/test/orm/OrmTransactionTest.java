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

import java.util.concurrent.CountDownLatch;

import org.lealone.test.SqlScript;
import org.lealone.test.UnitTestBase;
import org.lealone.test.generated.model.Customer;
import org.lealone.test.generated.model.Product;

public class OrmTransactionTest extends UnitTestBase {

    public static void main(String[] args) {
        new OrmTransactionTest().runTest();
    }

    @Override
    public void test() {
        SqlScript.createCustomerTable(this);
        SqlScript.createProductTable(this);

        // 测试在同一个线程中的多表事务
        try {
            Customer.dao().beginTransaction();

            new Customer().name.set("c1").phone.set(8001).insert();
            new Customer().name.set("c2").phone.set(8002).insert();

            new Product().productId.set(1001).productName.set("p1").insert();
            new Product().productId.set(1002).productName.set("p2").insert();

            Customer.dao().commitTransaction();
        } catch (Exception e) {
            Customer.dao().rollbackTransaction();
            e.printStackTrace();
        }

        // 测试在不同线程中的多表事务
        final CountDownLatch latch = new CountDownLatch(1);
        final long tid = Customer.dao().beginTransaction();

        try {
            new Customer().name.set("c3").phone.set(8003).insert();
            new Customer().name.set("c4").phone.set(8004).insert();

            new Thread(() -> {
                new Product().productId.set(1003).productName.set("p3").insert(tid); // 传递事务ID
                new Product().productId.set(1004).productName.set("p4").insert(tid);

                latch.countDown();
            }).start();

            latch.await();
            Customer.dao().commitTransaction(tid);
        } catch (Exception e) {
            Customer.dao().rollbackTransaction(tid);
            e.printStackTrace();
        }

        // 撤销事务
        try {
            Customer.dao().beginTransaction();
            new Customer().name.set("c5").phone.set(8005).insert();
            new Product().productId.set(1005).productName.set("p5").insert();
        } finally {
            Customer.dao().rollbackTransaction();
        }

        int count = Customer.dao().findCount();
        assertEquals(4, count);

        count = Product.dao().findCount();
        assertEquals(4, count);
    }

}
