/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.docdb;

import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.model.Filters;

public class BsonDocumentTest {

    public static void main(String[] args) {
        Document doc = Document.parse("{ status: { $in: [ \"A\", \"D\" ] } }");
        System.out.println(doc.toJson());

        Bson bson = Filters.and(Filters.eq("_id", 1), Filters.eq("_id", 2));
        System.out.println(bson.toBsonDocument().toJson());

        bson = Filters.and(Filters.gt("_id", 1), Filters.lt("_id", 2));
        System.out.println(bson.toBsonDocument().toJson());

        bson = Filters.in("_id", 1, 2, 2);
        System.out.println(bson.toBsonDocument().toJson());
        bson = Filters.nin("_id", 1, 2);
        System.out.println(bson.toBsonDocument().toJson());

        bson = Filters.type("_id", BsonType.DOUBLE);
        System.out.println(bson.toBsonDocument().toJson());
    }

}
