/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.lealone.db.async.Future;
import com.lealone.test.service.generated.CollectionTypeService;

public class CollectionTypeServiceImpl implements CollectionTypeService {

    @Override
    public List<Object> m1() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(100);
        list.add(200);
        return list;
    }

    @Override
    public List<Integer> m2() {
        return null;
    }

    @Override
    public Set<Object> m3() {
        return null;
    }

    @Override
    public Set<String> m4() {
        HashSet<String> set = new HashSet<>();
        set.add("abc");
        set.add("def");
        return set;
    }

    @Override
    public Map<Object, Object> m5() {
        return null;
    }

    // http://localhost:9000/service/collection_type_service/m6
    @Override
    public Map<Integer, String> m6() {
        HashMap<Integer, String> map = new HashMap<>();
        map.put(10, "a");
        map.put(20, "b");
        return map;
    }

    // http://localhost:9000/service/collection_type_service/m7?p1=1&p1=2&p2=a&p2=b
    @Override
    public Map<Integer, String> m7(List<Integer> p1, Set<String> p2, Map<Integer, String> p3,
            Integer p4) {
        System.out.println("p1: " + p1);
        System.out.println("p2: " + p2);
        System.out.println("p3: " + p3);
        System.out.println("p4: " + p4);
        return m6();
    }

    @Override
    public Future<List<Object>> m1Async() {
        return Future.succeededFuture(m1());
    }

    @Override
    public Future<List<Integer>> m2Async() {
        return Future.succeededFuture(m2());
    }

    @Override
    public Future<Set<Object>> m3Async() {
        return Future.succeededFuture(m3());
    }

    @Override
    public Future<Set<String>> m4Async() {
        return Future.succeededFuture(m4());
    }

    @Override
    public Future<Map<Object, Object>> m5Async() {
        return Future.succeededFuture(m5());
    }

    @Override
    public Future<Map<Integer, String>> m6Async() {
        return Future.succeededFuture(m6());
    }

    @Override
    public Future<Map<Integer, String>> m7Async(List<Integer> p1, Set<String> p2,
            Map<Integer, String> p3, Integer p4) {
        return Future.succeededFuture(m7(p1, p2, p3, p4));
    }
}
