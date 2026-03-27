/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.orm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.lealone.test.orm.generated.CollectionProperty;

public class CollectionPropertyTest extends OrmTestBase {
    @Before
    @Override
    public void setUpBefore() {
        setEmbedded(true);
        setInMemory(true);
    }

    @Test
    public void run() throws Exception {
        SqlScript.createCollectionPropertyTable(this);
        testCollectionType();
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

        CollectionProperty cp = new CollectionProperty();
        cp.f1.set(new ArrayList<Object>(list));
        cp.f2.set(list);
        cp.f3.set(new HashSet<Object>(set));
        cp.f4.set(set);
        cp.f5.set(new HashMap<Object, Object>(map));
        cp.f6.set(map);
        cp.insert();

        CollectionProperty cp2 = CollectionProperty.dao.findOne();
        assertTrue(cp, cp2);

        String json = cp.encode();
        cp2 = CollectionProperty.decode(json);
        assertTrue(cp, cp2);
    }

    void assertTrue(CollectionProperty cp, CollectionProperty cp2) {
        assertTrue(cp2.f1.get().equals(cp.f1.get()));
        assertTrue(cp2.f2.get().equals(cp.f2.get()));
        assertTrue(cp2.f3.get().equals(cp.f3.get()));
        assertTrue(cp2.f4.get().equals(cp.f4.get()));

        // 不能这么断言，因为 map 被编码为 json 后，key 变成字符串了
        // assertTrue(cp2.f5.get().equals(cp.f5.get()));
        HashMap<String, Object> map1 = new HashMap<>();
        HashMap<String, Object> map2 = new HashMap<>();
        for (Entry<Object, Object> e : cp.f5.get().entrySet()) {
            String key = e.getKey().toString();
            map1.put(key, e.getValue());
        }
        for (Entry<Object, Object> e : cp2.f5.get().entrySet()) {
            String key = e.getKey().toString();
            map2.put(key, e.getValue());
        }
        assertTrue(map1.equals(map2));

        assertTrue(cp2.f6.get().equals(cp.f6.get()));
    }
}
