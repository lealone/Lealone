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

import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.generated.User;

import io.vertx.core.json.JsonObject;

public class DaoTest extends UnitTestBase {

    public static void main(String[] args) {
        new DaoTest().runTest();
    }

    @Override
    public void test() {
        // 创建表: user
        execute("create table user(name char(10) primary key, notes varchar, phone int)" //
                + " package '" + DaoTest.class.getPackage().getName() + ".generated'" //
                + " generate code './src/test/java'");
        crud();
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

        // dao对象序列化后包含isDao字段，并且是true
        JsonObject json = JsonObject.mapFrom(User.dao);
        assertTrue(json.getBoolean("isDao"));

        // 反序列化
        String str = json.encode();
        User u = new JsonObject(str).mapTo(User.class);
        assertTrue(u.isDao());

        // 普通User对象序列化后也包含isDao字段，但为false
        json = JsonObject.mapFrom(new User());
        assertFalse(json.getBoolean("isDao"));
    }

}
