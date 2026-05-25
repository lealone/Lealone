/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.orm;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.lealone.orm.json.Json;
import com.lealone.orm.json.JsonArray;
import com.lealone.orm.json.JsonObject;
import com.lealone.orm.json.JsonParser;
import com.lealone.orm.json.codec.LealoneJsonCodec;

public class JsonTest extends OrmTestBase {

    @Before
    @Override
    public void setUpBefore() {
    }

    @Test
    public void run() throws Exception {
        Json.jsonCodec = Json.getJsonCodec();
        run0();
        Json.jsonCodec = new LealoneJsonCodec();
        run0();
    }

    private void run0() throws Exception {
        JsonArray array = new JsonArray();
        array.add("a").add(8.0);
        JsonObject jo = new JsonObject();
        jo.put("a", "ab");
        jo.put("b", 2);
        jo.put("c", array);
        String json = jo.encode();
        // System.out.println(json);
        JsonParser parser = new JsonParser();
        Map<String, Object> map = parser.parseJsonObject(json);
        // System.out.println(map);
        assertEquals(3, map.size());
        assertTrue(map.get("c") instanceof List);
        assertTrue(parser.parseJsonAny(array.encode()) instanceof List);

        assertEquals(3, parser.parseJsonAny("3"));
        assertEquals("3.5", parser.parseJsonAny("3.5").toString());
        assertTrue(parser.parseJsonAny("true") instanceof Boolean);

        // decode(json);
        // encode(json);
    }

    void decode(String json) throws Exception {
        Json.jsonCodec = new LealoneJsonCodec();
        Json.jsonCodec = Json.getJsonCodec();
        for (int n = 0; n < 100; n++) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                JsonObject.decode(json);
                // new org.apache.tomcat.util.json.JSONParser(json).parseObject();
            }
            System.out.println(System.currentTimeMillis() - t1);
        }
    }

    void encode(String json) throws Exception {
        Object obj = JsonObject.decode(json);
        Json.jsonCodec = new LealoneJsonCodec();
        Json.jsonCodec = Json.getJsonCodec();
        for (int n = 0; n < 100; n++) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < 100000; i++) {
                JsonObject.encode(obj);
            }
            System.out.println(System.currentTimeMillis() - t1);
        }
    }
}
