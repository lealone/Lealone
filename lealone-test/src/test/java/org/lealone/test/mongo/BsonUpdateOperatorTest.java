/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.mongo;

import java.sql.Date;
import java.util.ArrayList;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;

public class BsonUpdateOperatorTest extends MongoTestBase {

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

    private void assertUpdate(Bson update, String fieldName, Object expected) {
        System.out.println(update.toBsonDocument().toJson());
        Bson filter = Filters.eq("_id", 1);
        UpdateOptions updateOptions = new UpdateOptions();
        UpdateResult result = collection.updateOne(filter, update, updateOptions);
        assertEquals(1, result.getModifiedCount());

        MongoCursor<Document> cursor = collection.find(filter).iterator();
        try {
            while (cursor.hasNext()) {
                BsonDocument doc = BsonDocument.parse(cursor.next().toJson());
                if (expected == null)
                    assertTrue(doc.get(fieldName) instanceof BsonNull);
                else if (expected instanceof Integer)
                    assertEquals(expected, doc.get(fieldName).asInt32().getValue());
                else
                    assertEquals(expected.toString(), doc.get(fieldName).asString().getValue());
                break;
            }
        } finally {
            cursor.close();
        }
    }

    @Test
    public void testUpdate() {
        Bson u = Updates.set("f1", 100);
        assertUpdate(u, "f1", 100);

        u = Updates.currentDate("f2");
        assertUpdate(u, "f2", now);

        u = Updates.inc("f1", 200);
        assertUpdate(u, "f1", 300);

        u = Updates.mul("f1", 10);
        assertUpdate(u, "f1", 3000);

        // 只会保留第2个inc
        u = Updates.combine(Updates.inc("f1", 100), Updates.inc("f1", 200), Updates.currentDate("f2"));
        assertUpdate(u, "f1", 3200);

        u = Updates.min("f1", 20);
        assertUpdate(u, "f1", 20);

        u = Updates.max("f1", 200);
        assertUpdate(u, "f1", 200);

        u = Updates.rename("f1", "f10");
        assertUpdate(u, "f10", 200);

        u = Updates.unset("f10");
        assertUpdate(u, "f10", null);
    }
}
