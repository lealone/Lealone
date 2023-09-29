/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;

public class BCAggregate extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn) {
        Table table = findTable(doc, "aggregate", conn);
        long rowCount = 0;
        if (table != null) {
            BsonArray pipeline = doc.getArray("pipeline", null);
            if (pipeline != null) {
                for (int i = 0, size = pipeline.size(); i < size; i++) {
                    BsonDocument document = pipeline.get(i).asDocument();
                    BsonDocument group = document.getDocument("$group", null);
                    if (group != null) {
                        BsonDocument agg = group.getDocument("n", null);
                        if (agg != null && agg.containsKey("$sum")) {
                            ServerSession session = getSession(table.getDatabase(), conn);
                            rowCount = table.getRowCount(session);
                            session.close();
                            break;
                        }
                    }
                }
            }
        }
        BsonDocument document = new BsonDocument();
        BsonDocument cursor = new BsonDocument();
        append(cursor, "id", 0L);
        append(cursor, "ns",
                doc.getString("$db").getValue() + "." + doc.getString("aggregate").getValue());
        BsonArray documents = new BsonArray();
        BsonDocument agg = new BsonDocument();
        append(agg, "_id", 1);
        append(agg, "n", (int) rowCount);
        documents.add(agg);
        cursor.append("firstBatch", documents);
        document.append("cursor", cursor);
        setOk(document);
        return document;
    }
}
