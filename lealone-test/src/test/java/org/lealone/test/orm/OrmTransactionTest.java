/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.lealone.test.orm.generated.Product;
import org.lealone.test.orm.generated.User;

public class OrmTransactionTest extends OrmTestBase {

    @Test
    public void run() {
        SqlScript.createUserTable(this);
        SqlScript.createProductTable(this);

        // 测试在同一个线程中的多表事务
        try {
            User.dao.beginTransaction();

            new User().name.set("u1").phone.set(8001).insert();
            new User().name.set("u2").phone.set(8002).insert();

            new Product().productId.set(1001).productName.set("p1").insert();
            new Product().productId.set(1002).productName.set("p2").insert();

            User.dao.commitTransaction();
        } catch (Exception e) {
            User.dao.rollbackTransaction();
            e.printStackTrace();
        }

        // 测试在不同线程中的多表事务
        final CountDownLatch latch = new CountDownLatch(1);
        final long tid = User.dao.beginTransaction();

        try {
            new User().name.set("u3").phone.set(8003).insert();
            new User().name.set("u4").phone.set(8004).insert();

            new Thread(() -> {
                new Product().productId.set(1003).productName.set("p3").insert(tid); // 传递事务ID
                new Product().productId.set(1004).productName.set("p4").insert(tid);

                latch.countDown();
            }).start();

            latch.await();
            User.dao.commitTransaction(tid);
        } catch (Exception e) {
            User.dao.rollbackTransaction(tid);
            e.printStackTrace();
        }

        // 撤销事务
        try {
            User.dao.beginTransaction();
            new User().name.set("u5").phone.set(8005).insert();
            new Product().productId.set(1005).productName.set("p5").insert();
        } finally {
            User.dao.rollbackTransaction();
        }

        int count = User.dao.findCount();
        assertEquals(4, count);

        count = Product.dao.findCount();
        assertEquals(4, count);

        // 清除所有记录
        User.dao.delete();
        Product.dao.delete();

        // 测试穿插事务
        long tid1 = User.dao.beginTransaction();
        long tid2 = User.dao.beginTransaction();

        new User().name.set("u1").phone.set(8001).insert(tid1);
        new User().name.set("u2").phone.set(8002).insert(tid2);
        new Product().productId.set(1001).productName.set("p1").insert(tid1);
        new Product().productId.set(1002).productName.set("p2").insert(tid2);

        // 提交事务tid1
        User.dao.commitTransaction(tid1);
        // 撤销事务tid2
        User.dao.rollbackTransaction(tid2);

        // 都只成功写入一条
        count = User.dao.findCount();
        assertEquals(1, count);

        count = Product.dao.findCount();
        assertEquals(1, count);
    }
}
