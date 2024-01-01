/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.plugins.mongo;

import java.util.ArrayList;

import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;

public class BsonFilterTest extends MongoTestBase {

    @Before
    @Override
    public void before() {
        super.before();
        if (collection.countDocuments() == 0)
            insert();
    }

    private void insert() {
        ArrayList<Document> documents = new ArrayList<>(2);
        documents.add(new Document().append("_id", 1).append("f1", 10));
        documents.add(new Document().append("_id", 2).append("f1", 20));
        collection.insertMany(documents);
    }

    private void assertFilter(Bson filter, int expected) {
        System.out.println(filter.toBsonDocument().toJson());
        MongoCursor<Document> cursor = collection.find(filter).iterator();
        int actual = 0;
        try {
            while (cursor.hasNext()) {
                actual++;
                cursor.next();
            }
        } finally {
            cursor.close();
        }
        assertEquals(expected, actual);
    }

    @Test
    public void testFilter() {
        Bson f = Filters.eq("f1", 10);
        assertFilter(f, 1);

        f = Filters.ne("f1", 30);
        assertFilter(f, 2);

        f = Filters.gt("_id", 2);
        assertFilter(f, 0);

        f = Filters.gte("_id", 2);
        assertFilter(f, 1);

        f = Filters.lt("f1", 30);
        assertFilter(f, 2);

        f = Filters.lte("f1", 10);
        assertFilter(f, 1);

        f = Filters.in("_id", 1, 2, 2);
        assertFilter(f, 2);

        f = Document.parse("{ _id: { $in: [ 1, 2 ] } }");
        assertFilter(f, 2);

        f = Filters.nin("_id", 1, 3);
        assertFilter(f, 1);

        f = Filters.and(Filters.eq("_id", 1), Filters.eq("f1", 10));
        assertFilter(f, 1);

        f = Filters.or(Filters.eq("_id", 1), Filters.gte("_id", 2));
        assertFilter(f, 2);

        f = Filters.and(Filters.gt("_id", 3), f);
        assertFilter(f, 0);

        f = Filters.not(Filters.gt("_id", 2));
        assertFilter(f, 2);

        f = Filters.not(Filters.or(Filters.eq("_id", 1), Filters.eq("_id", 2)));
        assertFilter(f, 0);

        f = Filters.exists("_id");
        assertFilter(f, 2);

        f = Filters.type("_id", BsonType.DOUBLE);
        // assertFilter(f, 2);
    }
}
