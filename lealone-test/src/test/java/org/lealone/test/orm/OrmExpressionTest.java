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
import org.lealone.test.orm.generated.User;

public class OrmExpressionTest extends UnitTestBase {

    public static void main(String[] args) {
        new OrmExpressionTest().runTest();
    }

    @Override
    public void test() {
        SqlScript.createUserTable(this);
        testSelect();
        testOffsetLimit();
        testNot();
        testIn();
        testLike();
    }

    void testSelect() {
        User d = User.dao;
        // select name, phone from user where name = 'zhh' and (notes = 'notes1' or notes = 'notes2')
        // group by name, phone having name = 'zhh' order by name asc, phone desc;
        d = d.select(d.name, d.phone).where().name.eq("zhh").and().lp().notes.eq("notes1").or().notes.eq("notes2").rp()
                .groupBy(d.name, d.phone).having().name.eq("zhh").orderBy().name.asc().phone.desc();

        d.printSQL();
        d.findList();
    }

    void testOffsetLimit() {
        new User().id.set(1001).name.set("rob1").insert();
        new User().id.set(1002).name.set("rob2").insert();
        new User().id.set(1003).name.set("rob3").insert();
        new User().id.set(1004).name.set("rob4").insert();
        new User().id.set(1005).name.set("rob5").insert();

        List<User> list = User.dao.offset(2).findList();
        assertEquals(3, list.size());
        list = User.dao.limit(3).findList();
        assertEquals(3, list.size());
        list = User.dao.offset(1).limit(2).findList();
        assertEquals(2, list.size());
    }

    void testNot() {
        User d = User.dao.where().name.eq("zhh").and().not().notes.eq("notes1");
        d.printSQL();
        d.findList();

        d = User.dao.where().name.eq("zhh").and().not().lp().notes.eq("notes1").or().notes.eq("notes2").rp();
        d.printSQL();
        d.findList();

        d = User.dao.where().not().not().notes.eq("notes1");
        d.printSQL();
        d = User.dao.where().not().not().not().notes.eq("notes1");
        d.printSQL();
    }

    void testIn() {
        User d = User.dao.where().notes.in("notes1");
        d.printSQL();
        d.findList();

        d = User.dao.where().notes.notIn("notes1");
        d.printSQL();
        d.findList();

        d = User.dao.where().notes.in("notes1", "notes2");
        d.printSQL();
        d.findList();

        d = User.dao.where().notes.notIn("notes1", "notes2");
        d.printSQL();
        d.findList();
    }

    void testLike() {
        List<User> list = User.dao.where().name.eq("ROB1").findList();
        assertEquals(0, list.size());
        list = User.dao.where().name.ieq("ROB1").findList();
        assertEquals(1, list.size());

        list = User.dao.where().name.like("%ROB%").findList();
        assertEquals(0, list.size());
        list = User.dao.where().name.ilike("%ROB%").findList();
        assertEquals(5, list.size());

        list = User.dao.where().name.startsWith("ROB").findList();
        assertEquals(0, list.size());
        list = User.dao.where().name.istartsWith("ROB").findList();
        assertEquals(5, list.size());

        list = User.dao.where().name.endsWith("OB1").findList();
        assertEquals(0, list.size());
        list = User.dao.where().name.iendsWith("OB1").findList();
        assertEquals(1, list.size());

        list = User.dao.where().name.contains("OB").findList();
        assertEquals(0, list.size());
        list = User.dao.where().name.icontains("OB").findList();
        assertEquals(5, list.size());

        list = User.dao.where().name.match("[rob/d]").findList();
        assertEquals(5, list.size());
    }
}
