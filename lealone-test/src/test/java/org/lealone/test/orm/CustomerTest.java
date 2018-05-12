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

import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.generated.Customer;

import io.vertx.core.json.JsonObject;

public class CustomerTest extends UnitTestBase {

    public static void main(String[] args) {
        new CustomerTest().runTest();
    }

    @Override
    public void test() {
        createTable();
        crud();
        json();
    }

    void json() {
        Customer c = new Customer();
        c.name.set("Rob");

        JsonObject json = JsonObject.mapFrom(c);
        String str = json.encode();
        System.out.println("json: " + str);

        c = new JsonObject(str).mapTo(Customer.class);
        assertEquals("Rob", c.name.get());
    }

    void createTable() {
        System.out.println("create table: customer");

        execute("create table customer(id long, name char(10), notes varchar, phone int)" //
                + " package 'org.lealone.test.orm.generated'" //
                + " generate code './src/test/java'" // 生成领域模型类和查询器类的代码
        );
    }

    void crud() {
        System.out.println("crud test");
        String url = getURL();
        System.out.println("jdbc url: " + url);

        long rowId1;
        long rowId2;

        Customer c = Customer.create(url);

        // 增加两条记录
        rowId1 = c.id.set(1000).name.set("Rob1").notes.set("notes1").insert();

        assertTrue((rowId1 == 1) && (rowId1 == c._rowid_.get()));

        rowId2 = c.id.set(2000).name.set("Rob2").notes.set("notes2").insert();

        assertTrue((rowId2 == 2) && (rowId2 == c._rowid_.get()));

        // 以下出现的where()都不是必须的，加上之后更像SQL

        // 查找单条记录
        // select * from customer where id = 1000;
        c = c.where().id.eq(1000L).findOne();

        assertTrue((c.id.get() == 1000) && (1 == c._rowid_.get()));

        // 查找多条记录(取回所有字段)
        // select * from customer where name like 'Rob%';
        List<Customer> customers = c.where().name.like("Rob%").findList();

        assertEquals(2, customers.size());
        assertNotNull(customers.get(0).notes.get());

        // 查找多条记录(只取回name字段)
        // select name from customer where name like 'Rob%';
        customers = c.select(c.name).where().name.like("Rob%").findList();

        assertEquals(2, customers.size());
        assertNull(customers.get(0).notes.get());

        // 统计行数
        // select count(*) from customer where name like 'Rob%';
        int count = c.where().name.like("Rob%").findCount();

        assertEquals(2, count);

        assertEquals(1, c._rowid_.get());

        // 更新单条记录
        // update customer set notes = 'Doing an update' where _rowid_ = 1;
        c.notes.set("Doing an update");
        count = c.update();

        assertEquals(1, count);

        // 批量更新记录
        // update customer set phone = 12345678, notes = 'Doing a batch update' where name like 'Rob%';
        count = c.phone.set(12345678).notes.set("Doing a batch update").where().name.like("Rob%").update();

        assertEquals(2, count);

        // 删除单条记录
        // delete from customer where _rowid_ = 1;
        count = c.delete();

        assertEquals(1, count);

        // 批量删除记录
        // delete from customer where name like 'Rob%';
        count = c.where().name.like("Rob%").delete();

        assertEquals(1, count);
    }
}
