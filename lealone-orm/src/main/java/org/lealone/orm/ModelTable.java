/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.orm;

import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.session.ServerSession;
import org.lealone.db.session.ServerSessionFactory;
import org.lealone.db.table.Table;

public class ModelTable {

    private final String url;
    private final String databaseName;
    private final String schemaName;
    private final String tableName;

    private Table table;
    private ServerSession session;

    public ModelTable(String databaseName, String schemaName, String tableName) {
        this(null, databaseName, schemaName, tableName);
    }

    public ModelTable(String url, String databaseName, String schemaName, String tableName) {
        this.url = url;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
        if (url != null)
            attachToTable();
    }

    String getDatabaseName() {
        return databaseName;
    }

    String getSchemaName() {
        return schemaName;
    }

    String getTableName() {
        return tableName;
    }

    Table getTable() {
        attachToTable();
        return table;
    }

    ServerSession getSession() {
        attachToTable();
        return session;
    }

    Database getDatabase() {
        attachToTable();
        return table.getDatabase();
    }

    public ModelTable copy() {
        return new ModelTable(url, databaseName, schemaName, tableName);
    }

    // 可能是延迟关联到Table
    private void attachToTable() {
        if (table == null) {
            String url = System.getProperty("lealone.jdbc.url");
            if (url == null) {
                // 默认用嵌入式
                url = Constants.URL_PREFIX + Constants.URL_EMBED + databaseName + ";password=;user=root";
                // throw new RuntimeException("'lealone.jdbc.url' must be set");
            }
            session = (ServerSession) ServerSessionFactory.getInstance().createSession(url).get();
            Database db = session.getDatabase();

            // if (db.getSettings().databaseToUpper) {
            // tableName = tableName.toUpperCase();
            // }
            // int dotPos = tableName.indexOf('.');
            // String schemaName = Constants.SCHEMA_MAIN;
            // if (dotPos > -1) {
            // schemaName = tableName.substring(0, dotPos);
            // tableName = tableName.substring(dotPos + 1);
            // }
            table = db.getSchema(session, schemaName).getTableOrView(session, tableName);
        }
    }
}
