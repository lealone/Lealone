/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mongo;

import java.util.ArrayList;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public class MongoCrudTest extends MongoTestBase {

    public static void main(String[] args) {
        new MongoCrudTest().runTest();
    }

    public MongoCrudTest() {
        super("c1");
    }

    @Override
    protected void test() throws Exception {
        crud();
    }

    @Test
    public void crud() {
        collection.drop();
        insert();
        query();
        update();
        delete();
    }

    static int id = 0;

    static Document createDocument(int f1, int f2) {
        return new Document().append("_id", ++id).append("f1", f1).append("f2", f2);
    }

    void insert() {
        Document doc = createDocument(1, 2);
        InsertOneResult r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());
        doc = createDocument(10, 20);
        r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());

        ArrayList<Document> documents = new ArrayList<>();
        documents.add(createDocument(11, 21));
        documents.add(createDocument(12, 22));
        // 测试列不匹配的情况
        documents.add(new Document().append("_id", ++id).append("f1", 111));
        collection.insertMany(documents);

        long count = collection.countDocuments();
        System.out.println("total document count: " + count);
        assertEquals(5, count);
    }

    void delete() {
        Bson filter = Filters.eq("_id", 1);
        DeleteOptions options = new DeleteOptions();
        DeleteResult result = collection.deleteOne(filter, options);
        System.out.println("DeletedCount: " + result.getDeletedCount());
        assertEquals(1, result.getDeletedCount());
    }

    void update() {
        Bson filter = Filters.eq("_id", 1);
        Bson update = Updates.set("f1", 100);
        UpdateOptions updateOptions = new UpdateOptions();
        UpdateResult result = collection.updateOne(filter, update, updateOptions);
        System.out.println("ModifiedCount: " + result.getModifiedCount());
    }

    void query() {
        MongoCursor<Document> cursor = collection.find(Filters.eq("f1", 1))
                .projection(Projections.include("f2")).iterator();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }
}
