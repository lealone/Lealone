/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.mongo;

import java.util.ArrayList;
import java.util.Arrays;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;

public class BsonAdminCommandTest extends MongoTestBase {

    @Before
    @Override
    public void before() {
        super.before();
        database.getCollection("view1").drop();
        database.getCollection("a_empty_collection").drop();
        collection.drop();
        insert();
    }

    private void insert() {
        ArrayList<Document> documents = new ArrayList<>(2);
        documents.add(new Document().append("_id", 1).append("f1", 10).append("f2", 1));
        documents.add(new Document().append("_id", 2).append("f1", 20).append("f2", 2));
        collection.insertMany(documents);
    }

    @Test
    public void testAdminCommand() {
        database.createCollection("a_empty_collection");
        Bson project = Aggregates.project(Projections.include("f1", "f2"));
        database.createView("view1", collectionName, Arrays.asList(project));
    }
}
