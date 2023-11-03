/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mongo;

import org.bson.Document;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.InsertOneResult;

public class MongoIdTest extends MongoTestBase {

    @Test
    public void crud() {
        collection.drop();
        insert();
        query();
    }

    static Document createDocument(int f1, int f2) {
        return new Document().append("f1", f1).append("f2", f2);
    }

    void insert() {
        Document doc = createDocument(1, 2);
        InsertOneResult r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());
        doc = createDocument(10, 20);
        r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());

        long count = collection.countDocuments();
        System.out.println("total document count: " + count);
        assertEquals(2, count);
    }

    void query() {
        MongoCursor<Document> cursor = collection.find(Filters.eq("f1", 1))
                .projection(Projections.include("_id")).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }
}
