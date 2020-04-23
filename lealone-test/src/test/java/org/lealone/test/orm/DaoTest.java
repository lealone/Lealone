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

import org.lealone.orm.json.JsonObject;
import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.generated.User;

public class DaoTest extends UnitTestBase {

    public static void main(String[] args) {
        new DaoTest().runTest();
    }

    @Override
    public void test() {
        SqlScript.createUserTable(this);
        testMultiThreads();
        crud();
    }

    // 测试多个线程同时使用User.dao是否产生混乱
    void testMultiThreads() {
        final CountDownLatch latch = new CountDownLatch(2);

        new User().name.set("zhh1").insert();
        new User().name.set("zhh2").insert();
        new Thread(() -> {
            User.dao.where().name.eq("zhh1").delete();
            latch.countDown();
        }).start();

        new Thread(() -> {
            User.dao.where().name.eq("zhh2").delete();
            latch.countDown();
        }).start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        int count = User.dao.findCount();
        assertEquals(0, count);
    }

    void crud() {
        // 只有User.dao才能执行findXXX
        try {
            new User().findOne();
            fail();
        } catch (RuntimeException e) {
        }
        try {
            new User().findList();
            fail();
        } catch (RuntimeException e) {
        }
        try {
            new User().findCount();
            fail();
        } catch (RuntimeException e) {
        }

        // OK
        new User().name.set("zhh").insert();

        // 没有指定主键
        try {
            new User().update();
            fail();
        } catch (RuntimeException e) {
        }

        // 没有指定主键
        try {
            new User().phone.set(123).update();
            fail();
        } catch (RuntimeException e) {
        }

        // OK
        new User().name.set("zhh").update();

        // OK 取回所有字段
        User user = User.dao.where().name.eq("zhh").findOne();
        user.phone.set(123);
        user.update();

        // 也OK，虽然只取回phone字段，虽然没有name主键，但有_rowid_
        user = User.dao.select(User.dao.phone).where().name.eq("zhh").findOne();
        user.notes.set("dao test");
        user.update();

        // User.dao不允许执行insert
        try {
            User.dao.name.set("zhh").insert();
            fail();
        } catch (RuntimeException e) {
        }

        // 没有指定主键
        try {
            new User().delete();
            fail();
        } catch (RuntimeException e) {
        }

        // 没有指定主键
        try {
            new User().phone.set(123).delete();
            fail();
        } catch (RuntimeException e) {
        }

        // OK
        new User().name.set("zhh").delete();

        // dao对象序列化后包含modelType字段，并且是ROOT_DAO
        JsonObject json = JsonObject.mapFrom(User.dao);
        assertTrue(json.getInteger("modelType") == User.ROOT_DAO);

        // 反序列化
        String str = json.encode();
        User u = new JsonObject(str).mapTo(User.class);
        assertTrue(u.isDao());

        // 普通User对象序列化后也包含modelType字段，但为REGULAR_MODEL
        json = JsonObject.mapFrom(new User());
        assertTrue(json.getInteger("modelType") == User.REGULAR_MODEL);
    }
}
