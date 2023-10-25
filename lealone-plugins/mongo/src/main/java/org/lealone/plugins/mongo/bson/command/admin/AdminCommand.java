/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.bson.command.admin;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.io.ByteBufferBsonInput;
import org.lealone.db.Database;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.table.Table;
import org.lealone.db.table.TableView;
import org.lealone.plugins.mongo.bson.command.BCFind;
import org.lealone.plugins.mongo.bson.command.BsonCommand;
import org.lealone.plugins.mongo.server.MongoServerConnection;
import org.lealone.plugins.mongo.server.MongoTask;

public abstract class AdminCommand extends BsonCommand {

    public static BsonDocument execute(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, String command, MongoTask task) {
        switch (command) {
        case "create":
            return ACCreate.execute(input, doc, conn, task);
        case "drop":
            return ACDrop.execute(input, doc, conn, task);
        case "getParameter": {
            BsonDocument document = new BsonDocument();
            BsonDocument v = new BsonDocument();
            append(v, "version", "6.0");
            document.append("featureCompatibilityVersion", v);
            setOk(document);
            return document;
        }
        case "listDatabases":
            return listDatabases(input, doc, conn, task);
        case "listCollections":
            return listCollections(input, doc, conn, task);
        case "atlasVersion":
            return newOkBsonDocument();
        default:
            return NOT_FOUND;
        }
    }

    private static BsonDocument listDatabases(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        BsonArray databases = new BsonArray();
        for (Database db : LealoneDatabase.getInstance().getDatabases()) {
            BsonDocument collection = new BsonDocument();
            collection.append("name", new BsonString(db.getName()));
            databases.add(collection);
        }
        BsonDocument document = new BsonDocument();
        document.append("databases", databases);
        setOk(document);
        return document;
    }

    private static BsonDocument listCollections(ByteBufferBsonInput input, BsonDocument doc,
            MongoServerConnection conn, MongoTask task) {
        Database db = getDatabase(doc);
        BsonArray collections = new BsonArray();
        for (Table t : db.getAllTablesAndViews(false)) {
            BsonDocument collection = new BsonDocument();
            collection.append("name", new BsonString(t.getName()));
            collection.append("type", new BsonString((t instanceof TableView) ? "view" : "collection"));
            collections.add(collection);
        }
        return BCFind.createResponseDocument(doc, collections, "$cmd.listCollections");
    }
}
