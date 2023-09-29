/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server.command;

import org.bson.BsonDocument;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.index.Cursor;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Table;
import org.lealone.docdb.server.DocDBServerConnection;

public class BCDelete extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            DocDBServerConnection conn) {
        int n = 0;
        Table table = getTable(doc, "delete", conn);
        try (ServerSession session = getSession(table.getDatabase(), conn)) {
            Cursor cursor = table.getScanIndex(session).find(session, null, null);
            while (cursor.next()) {
                table.removeRow(session, cursor.get());
                n++;
            }
            session.commit();
        }
        BsonDocument document = new BsonDocument();
        setOk(document);
        setN(document, n);
        return document;
    }
}
