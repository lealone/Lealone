/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server.bson.command.index;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.index.Index;
import org.lealone.db.index.IndexColumn;
import org.lealone.db.result.SortOrder;
import org.lealone.db.table.Table;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;

public class ICListIndexes extends IndexCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Table table = findTable(doc, "listIndexes", conn);
        BsonArray indexes = new BsonArray();
        if (table != null) {
            for (Index index : table.getIndexes()) {
                if (index.isRowIdIndex())
                    continue;
                BsonDocument indexDoc = new BsonDocument();
                append(indexDoc, "v", 2);
                BsonDocument keyDoc = new BsonDocument();
                for (IndexColumn ic : index.getIndexColumns()) {
                    append(keyDoc, ic.columnName, ic.sortType == SortOrder.ASCENDING ? 1 : -1);
                }
                indexDoc.append("key", keyDoc);
                append(indexDoc, "name", index.getName());
                indexes.add(indexDoc);
            }
        }
        return createResponseDocument(doc, indexes);
    }

    private static BsonDocument createResponseDocument(BsonDocument doc, BsonArray documents) {
        BsonDocument document = new BsonDocument();
        BsonDocument cursor = new BsonDocument();
        append(cursor, "id", 0L);
        append(cursor, "ns",
                doc.getString("$db").getValue() + "." + doc.getString("listIndexes").getValue());
        cursor.append("firstBatch", documents);
        document.append("cursor", cursor);
        setOk(document);
        return document;
    }
}
