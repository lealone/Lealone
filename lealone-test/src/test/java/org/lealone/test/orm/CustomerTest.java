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

import org.lealone.orm.Database;
import org.lealone.orm.Table;
import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.generated.Customer;
import org.lealone.test.orm.generated.QCustomer;

public class CustomerTest extends UnitTestBase {

    private static final String url = "jdbc:lealone:embed:test;user=root;password=root;persistent=false";

    public static void main(String[] args) {
        new CustomerTest().run();
    }

    private Database db;

    public void run() {
        createTable();
        crud();
        db.close();
    }

    private void createTable() {
        System.out.println("create table");
        // 先建表
        db = new Database(url);
        db.executeUpdate("create table customer(id long, name char(10), notes varchar, phone int)");

        System.out.println("gen java jode");
        Table t = db.getTable("customer");
        // 生成领域模型类和查询器类的代码
        t.genJavaCode("./src/test/java", "org.lealone.test.orm.generated");
    }

    private void crud() {
        System.out.println("crud test");

        // 增加两条记录
        Customer.create(url).setName("Rob1").save();
        Customer.create(url).setName("Rob2").save();

        // 构建一个查询器
        QCustomer qCustomer = QCustomer.create(url);

        // 以下出现的where()都不是必须的，加上之后更像SQL

        // 查找单条记录
        // select * from customer where id = 1;
        // Customer rob = qCustomer.where().id.eq(1L).findOne();

        // 查找多条记录(取回所有字段)
        // select * from customer where name like 'Rob%';
        List<Customer> customers = qCustomer.where().name.like("Rob%").findList();

        // 查找多条记录(只取回name字段)
        // select name from customer where name like 'Rob%';
        customers = qCustomer.select(qCustomer.name).where().name.like("Rob%").findList();
        customers.size();

        // 统计行数
        // select count(*) from customer where name like 'Rob%';
        qCustomer.where().name.like("Rob%").findCount();

        // 更新单条记录
        // update customer set notes = 'Doing an update' where id = 1;
        // rob.setNotes("Doing an update");
        // rob.save();

        // 批量更新记录
        // update customer set phone = 12345678, notes = 'Doing an batch update' where name like 'Rob%';
        qCustomer.phone.set(12345678).notes.set("Doing an batch update").where().name.like("Rob%").update();

        // 删除单条记录
        // delete from customer where id = 1;
        // rob.delete();

        // 批量删除记录
        // delete from customer where name like 'Rob%';
        qCustomer.where().name.like("Rob%").delete();
    }
}
