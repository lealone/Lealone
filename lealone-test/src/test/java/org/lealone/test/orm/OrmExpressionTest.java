/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import java.util.List;

import org.junit.Test;
import org.lealone.test.UnitTestBase;
import org.lealone.test.orm.generated.User;

public class OrmExpressionTest extends UnitTestBase {

    @Test
    public void run() {
        SqlScript.createUserTable(this);
        testSelect();
        testOffsetLimit();
        testNot();
        testIn();
        testLike();
        testArray();
        testSet();
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

    void testArray() {
        new User().id.set(1006).name.set("rob6").phones.set(new Object[] { 1, 2, 3 }).insert();

        List<User> list = User.dao.where().id.eq(1006).findList();
        assertEquals(1, list.size());

        list = User.dao.where().id.eq(1006).phones.isEmpty().findList();
        assertEquals(0, list.size());

        list = User.dao.where().id.eq(1006).phones.isNotEmpty().findList();
        assertEquals(1, list.size());

        list = User.dao.where().id.eq(1006).phones.contains(1).findList();
        assertEquals(1, list.size());

        list = User.dao.where().id.eq(1006).phones.contains(1, 3).findList();
        assertEquals(1, list.size());

        list = User.dao.where().id.eq(1006).phones.notContains(4).findList();
        assertEquals(1, list.size());

        list = User.dao.where().id.eq(1006).phones.notContains(4, 3).findList();
        assertEquals(1, list.size());
    }

    void testSet() {
        try {
            // 必须设置字段值
            new User().insert();
            fail();
        } catch (UnsupportedOperationException e) {
        }
        // 同一字段多次set只取最后一次
        User user = new User().id.set(9000).name.set("rob6").name.set("rob7");
        assertEquals("rob7", user.name.get());
        user.insert();
        user = User.dao.where().id.eq(9000).findOne();
        assertEquals("rob7", user.name.get());
        // 没有变化，直接返回0
        assertEquals(0, user.name.set("rob7").update());
    }
}
