/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mongo;

import java.sql.Date;
import java.util.ArrayList;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

public class BsonIndexTest extends MongoTestBase {

    private final Date now = new Date(System.currentTimeMillis());

    @Before
    @Override
    public void before() {
        super.before();
        collection.drop();
        insert();
    }

    private void insert() {
        ArrayList<Document> documents = new ArrayList<>(2);
        documents.add(new Document().append("_id", 1).append("f1", 10).append("f2", now));
        documents.add(new Document().append("_id", 2).append("f1", 20).append("f2", now));
        collection.insertMany(documents);
    }

    @Test
    public void testIndex() {
        collection.createIndex(Indexes.descending("f1"));
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.name("i_f1");
        collection.createIndex(Indexes.ascending("f2"), indexOptions);
        collection.listIndexes().forEach(doc -> System.out.println(doc.toJson()));
        collection.dropIndex("i_f1");
        collection.dropIndexes();
    }
}
