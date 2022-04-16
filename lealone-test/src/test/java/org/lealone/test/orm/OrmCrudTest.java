/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.orm;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.Test;
import org.lealone.test.orm.generated.User;

public class OrmCrudTest extends OrmTestBase {

    @Test
    public void run() {
        SqlScript.createUserTable(this);
        crud();
        json();
    }

    void json() {
        User u = new User();
        u.name.set("Rob").phones.set(new Object[] { 1, 2, 3 });
        String str = u.encode();
        System.out.println("json: " + str);

        u = User.decode(str);
        assertEquals("Rob", u.name.get());
        System.out.println("phones: " + u.phones.get().length);
    }

    private long getRowId(Object obj) {
        Field f;
        try {
            f = obj.getClass().getSuperclass().getDeclaredField("_rowid_");
            f.setAccessible(true);

            Object obj2 = f.get(obj);
            Method m = obj2.getClass().getMethod("get", new Class[0]);
            m.setAccessible(true);
            return (long) m.invoke(obj2, new Object[0]);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void crud() {
        System.out.println("crud test");
        String url = getURL();
        System.out.println("jdbc url: " + url);

        long rowId1;
        long rowId2;

        User dao = User.dao;

        User u = new User();

        // 增加两条记录
        rowId1 = u.id.set(1000).name.set("Rob1").notes.set("notes1").insert();

        assertTrue((rowId1 == 1) && (rowId1 == getRowId(u)));

        rowId2 = u.id.set(2000).name.set("Rob2").notes.set("notes2").insert();

        assertTrue((rowId2 == 2) && (rowId2 == getRowId(u)));

        // 以下出现的where()都不是必须的，加上之后更像SQL

        // 查找单条记录
        // select * from user where id = 1000;
        u = dao.where().id.eq(1000L).findOne();

        assertTrue((u.id.get() == 1000) && (1 == getRowId(u)));

        // 查找多条记录(取回所有字段)
        // select * from user where name like 'Rob%';
        List<User> customers = dao.where().name.like("Rob%").findList();

        assertEquals(2, customers.size());
        assertNotNull(customers.get(0).notes.get());

        // 查找多条记录(只取回name字段)
        // select name from user where name like 'Rob%';
        customers = dao.select(dao.name).where().name.like("Rob%").findList();

        assertEquals(2, customers.size());
        assertNull(customers.get(0).notes.get());

        // 统计行数
        // select count(*) from user where name like 'Rob%';
        int count = dao.where().name.like("Rob%").findCount();

        assertEquals(2, count);

        assertEquals(1, getRowId(u));

        // 更新单条记录
        // update user set notes = 'Doing an update' where _rowid_ = 1;
        u.notes.set("Doing an update");
        count = u.update();

        assertEquals(1, count);

        // 批量更新记录
        // update user set phone = 12345678, notes = 'Doing a batch update' where name like 'Rob%';
        count = dao.phone.set(12345678).notes.set("Doing a batch update").where().name.like("Rob%").update();

        assertEquals(2, count);

        // 删除单条记录
        // delete from user where _rowid_ = 1;
        count = u.delete();

        assertEquals(1, count);

        // 批量删除记录
        // delete from user where name like 'Rob%';
        count = dao.where().name.like("Rob%").delete();

        assertEquals(1, count);
    }
}
