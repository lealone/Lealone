/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Test;

import com.lealone.db.Constants;
import com.lealone.db.LealoneDatabase;
import com.lealone.test.orm.SqlScript;
import com.lealone.test.service.generated.CollectionTypeService;
import com.lealone.test.sql.SqlTestBase;

public class CollectionTypeServiceTest extends SqlTestBase {

    public CollectionTypeServiceTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void testService() throws Exception {
        String url = getURL();
        // 设置jdbc url后，创建服务可以不用传url
        System.setProperty(Constants.JDBC_URL_KEY, url);
        SqlScript.createCollectionTypeService(this);
        CollectionTypeService service = CollectionTypeService.create();
        ArrayList<Integer> list = new ArrayList<>();
        list.add(100);
        list.add(200);
        HashSet<String> set = new HashSet<>();
        set.add("abc");
        set.add("def");
        HashMap<Integer, String> map = new HashMap<>();
        map.put(10, "a");
        map.put(20, "b");
        service.m7(list, set, map, 1);
    }
}
