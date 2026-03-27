/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm;

import com.lealone.db.ConnectionInfo;
import com.lealone.db.Constants;
import com.lealone.db.Database;
import com.lealone.db.session.ServerSession;
import com.lealone.db.table.Table;

public class ModelTable {

    private final String url;
    private final String databaseName;
    private final String schemaName;
    private final String tableName;

    // 这两个字段延后初始化
    private ServerSession session;
    private Table table;

    public ModelTable(String databaseName, String schemaName, String tableName) {
        this(null, databaseName, schemaName, tableName);
    }

    public ModelTable(String url, String databaseName, String schemaName, String tableName) {
        this.url = url;
        this.databaseName = databaseName;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    ModelTable copy() {
        return new ModelTable(url, databaseName, schemaName, tableName);
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

    Database getDatabase() {
        bindTable();
        return table.getDatabase();
    }

    ServerSession getSession() {
        bindTable();
        return session;
    }

    Table getTable() {
        bindTable();
        return table;
    }

    private void bindTable() {
        // 沒有初始化，或已经无效了，比如drop table后还被引用
        if (table == null || table.isInvalid()) {
            session = (ServerSession) new ConnectionInfo(getUrl()).createSession();
            Database db = session.getDatabase();
            table = db.getSchema(session, schemaName).getTableOrView(session, tableName);
        }
    }

    private String getUrl() {
        String url = this.url;
        if (url == null)
            url = System.getProperty(Constants.JDBC_URL_KEY);
        // 默认用嵌入式
        if (url == null || url.contains(Constants.URL_TCP))
            url = Constants.URL_PREFIX + Constants.URL_EMBED + databaseName + ";password=;user=root";
        return url;
    }
}
