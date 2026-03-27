/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.orm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.lealone.orm.format.JsonFormat;
import com.lealone.orm.json.JsonArray;
import com.lealone.orm.json.JsonObject;
import com.lealone.test.orm.generated.JsonTestTable;
import com.lealone.test.orm.generated.User;

public class OrmJsonTest extends OrmTestBase {

    private String oldJsonFormat;

    @Before
    @Override
    public void setUpBefore() {
        setEmbedded(true);
        setInMemory(true);
        SqlScript.createUserTable(this);
        SqlScript.createJsonTestTable(this);
        oldJsonFormat = System.getProperty("lealone.orm.json.format");
        System.setProperty("lealone.orm.json.format", "DEFAULT_FORMAT");
    }

    @After
    public void setUpAfter() {
        if (oldJsonFormat != null)
            System.setProperty("lealone.orm.json.format", oldJsonFormat);
        else
            System.setProperty("lealone.orm.json.format", "FRONTEND_FORMAT");
    }

    @Test
    public void run() {
        testModelType();
        testJsonFormat();
        testCollectionType();
    }

    void testModelType() {
        // dao对象序列化后包含modelType字段，并且是ROOT_DAO
        JsonObject json = new JsonObject(User.dao.encode());
        assertTrue(json.getInteger("modelType") == User.ROOT_DAO);

        // 反序列化
        String str = json.encode();
        User u = User.decode(str);
        assertTrue(u.isDao());

        // 普通User对象序列化后也包含modelType字段，但为REGULAR_MODEL
        json = new JsonObject(new User().encode());
        assertTrue(json.getInteger("modelType") == User.REGULAR_MODEL);
    }

    void testJsonFormat() {
        User user = new User().id.set(1006).name.set("rob6").phones.set(new Object[] { 1, 2, 3 });
        String json = user.encode();
        JsonObject jsonObject = new JsonObject(json);
        assertEquals(jsonObject.getString(User.dao.name.getName()), user.name.get());
        User u = User.decode(json);
        assertEquals(u.name.get(), user.name.get());

        JsonFormat jsonFormat = JsonFormat.FRONTEND_FORMAT;
        json = user.encode(jsonFormat);
        jsonObject = new JsonObject(json);
        String name = jsonFormat.getNameCaseFormat().convert(User.dao.name.getName());
        assertEquals(jsonObject.getString(name), user.name.get());

        JsonTestTable t1 = new JsonTestTable().propertyName1.set(1).propertyName2.set(3).b.set(true);
        json = t1.encode();
        jsonObject = new JsonObject(json);
        name = JsonFormat.LOWER_UNDERSCORE_FORMAT.getNameCaseFormat()
                .convert(JsonTestTable.dao.propertyName1.getName());
        assertEquals(jsonObject.getInteger(name), t1.propertyName1.get());
        JsonTestTable t2 = JsonTestTable.decode(json);
        assertEquals(t1.propertyName2.get(), t2.propertyName2.get());

        jsonFormat = JsonFormat.FRONTEND_FORMAT;
        json = t1.encode(jsonFormat);
        t2 = JsonTestTable.decode(json, jsonFormat);
        assertEquals(t1.propertyName2.get(), t2.propertyName2.get());
        assertTrue(t2.b.get());
    }

    void testCollectionType() {
        ArrayList<Integer> list = new ArrayList<>();
        list.add(100);
        list.add(200);
        HashSet<String> set = new HashSet<>();
        set.add("abc");
        set.add("def");
        HashMap<Integer, String> map = new HashMap<>();
        map.put(10, "a");
        map.put(20, "b");

        ArrayList<Object> list2 = new ArrayList<>();
        list2.add(list);
        list2.add(set);
        list2.add(map);

        JsonArray ja = new JsonArray(list2);
        String json = ja.encode();
        System.out.println(json);

        ja = new JsonArray(json);
        assertTrue(list.equals(ja.getList(0)));
        assertTrue(set.equals(ja.getSet(1)));
        assertTrue(map.equals(ja.getMap(2, Integer.class)));
        System.out.println(ja);
    }
}
