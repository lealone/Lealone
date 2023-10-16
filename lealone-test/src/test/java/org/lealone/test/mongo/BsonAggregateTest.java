/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.mongo;

import static com.mongodb.client.model.mql.MqlValues.current;
import static com.mongodb.client.model.mql.MqlValues.of;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.mql.MqlValue;

public class BsonAggregateTest extends MongoTestBase {

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
    public void testAggregate() {
        MqlValue e = current().getInteger("f1").eq(of(10));
        e = current().getInteger("_id").eq(of(1));
        // e = current().getString("_id").eq(of("1")); // 不能用getString，因为_id是Integer类型
        Bson bson = Filters.eq("_id", "1");
        bson = Filters.expr(e);
        printBson(bson);
        printBson(Aggregates.match(bson));

        collection
                .aggregate(Arrays.asList(Aggregates.match(bson),
                        Aggregates.group("$f1", Accumulators.sum("f1", "f1"))))
                .forEach(doc -> System.out.println(doc.toJson()));

        // collection.find(bson).forEach(doc -> System.out.println(doc.toJson()));
    }
}
