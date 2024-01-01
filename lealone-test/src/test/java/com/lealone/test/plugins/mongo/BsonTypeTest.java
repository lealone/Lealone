/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mongo;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.InsertOneResult;

//各种类型的用法参考: https://www.mongodb.com/docs/mongodb-shell/reference/data-types/
public class BsonTypeTest extends MongoTestBase {

    @Before
    @Override
    public void before() {
        super.before();
        collection.drop();
    }

    @Test
    public void testDocumentType() {
        insert();
        query();
    }

    // @Test
    // public void testDateTimeType() {
    // }

    static Document createDocument(int f1, String f2) {
        String json = "{f1: " + f1 + ",f2: " + f2 + "}";
        return Document.parse(json);
    }

    void insert() {
        Document doc = createDocument(1, "{a:1,b:2}");
        InsertOneResult r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());
        doc = createDocument(10, "{a:1,b:2,c:{c1:10,c2:2}}");
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
                System.out.println(doc.toJson());
            }
        } finally {
            cursor.close();
        }
    }
}
