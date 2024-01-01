/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mongo;

import java.util.List;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.InsertOneResult;

public class MongoArrayTest extends MongoTestBase {

    @Test
    public void crud() {
        collection.drop();
        insert();
        query();
    }

    static Document createDocument(int f1, String f2) {
        String json = "{f1: " + f1 + ",f2: " + f2 + "}";
        return Document.parse(json);
    }

    void insert() {
        Document doc = createDocument(1, "[1,2,\"a\"]");
        InsertOneResult r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());
        doc = createDocument(10, "[1,2,[10,20]]");
        r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());

        long count = collection.countDocuments();
        System.out.println("total document count: " + count);
        assertEquals(2, count);
    }

    void query() {
        MongoCursor<Document> cursor = collection.find(Filters.eq("f1", 10))
                .projection(Projections.include("f2")).iterator();
        try {
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                Object obj = doc.get("f2");
                assertTrue(obj instanceof List);
                List<?> list = (List<?>) obj;
                assertEquals(3, list.size());
                obj = list.get(2);
                assertTrue(obj instanceof List);
                list = (List<?>) obj;
                assertEquals(2, list.size());
                assertEquals(20, list.get(1));
                System.out.println(doc.toJson());
            }
        } finally {
            cursor.close();
        }
    }
}
