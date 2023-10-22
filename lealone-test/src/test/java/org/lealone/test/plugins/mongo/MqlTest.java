/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mongo;

import static com.mongodb.client.model.mql.MqlValues.current;
import static com.mongodb.client.model.mql.MqlValues.of;

import org.bson.conversions.Bson;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.mql.MqlValue;

public class MqlTest {

    public static void main(String[] args) {
        MqlValue e = current().getArray("visitDates").size().gt(of(0))
                .and(current().getString("state").eq(of("New Mexico")));
        println(e);
    }

    static void println(MqlValue e) {
        Bson bson = Filters.expr(e);
        System.out.println(
                bson.toBsonDocument(bson.getClass(), MongoClientSettings.getDefaultCodecRegistry())
                        .toJson());
    }
}
