/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.docdb;

import java.util.ArrayList;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.DeleteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;

public class DocDBCrudTest {

    public static void main(String[] args) {
        int port = 9610;
        // port = 27017;
        String connectionString = "mongodb://127.0.0.1:" + port + "/?serverSelectionTimeoutMS=200000";
        MongoClient mongoClient = MongoClients.create(connectionString);
        MongoDatabase database = mongoClient.getDatabase("docdb1");
        // System.out.println(database.runCommand(Document.parse("{\"buildInfo\": 1}")));
        // database.createCollection("c1");
        MongoCollection<Document> collection = database.getCollection("c1");
        collection.drop();
        insert(collection);
        query(collection);
        update(collection);
        delete(collection);
        mongoClient.close();
    }

    static int id = 0;

    static Document createDocument(int f1, int f2) {
        return new Document().append("_id", ++id).append("f1", f1).append("f2", f2);
    }

    static void insert(MongoCollection<Document> collection) {
        Document doc = createDocument(1, 2);
        InsertOneResult r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());
        doc = createDocument(10, 20);
        r = collection.insertOne(doc);
        System.out.println("InsertedId: " + r.getInsertedId());

        ArrayList<Document> documents = new ArrayList<>();
        documents.add(createDocument(11, 21));
        documents.add(createDocument(12, 22));
        collection.insertMany(documents);

        long count = collection.countDocuments();
        System.out.println("total document count: " + count);
    }

    static void delete(MongoCollection<Document> collection) {
        Bson filter = Filters.eq("_id", 1);
        DeleteOptions options = new DeleteOptions();
        DeleteResult result = collection.deleteOne(filter, options);
        System.out.println("DeletedCount: " + result.getDeletedCount());
    }

    static void update(MongoCollection<Document> collection) {
        Bson filter = Filters.eq("_id", 1);
        Bson update = Updates.addToSet("f1", 100);
        UpdateOptions updateOptions = new UpdateOptions();
        UpdateResult result = collection.updateOne(filter, update, updateOptions);
        System.out.println("ModifiedCount: " + result.getModifiedCount());
    }

    static void query(MongoCollection<Document> collection) {
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
