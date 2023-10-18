/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server;

import java.util.Map;

import org.lealone.db.LealoneDatabase;
import org.lealone.net.WritableChannel;
import org.lealone.server.AsyncServer;
import org.lealone.server.Scheduler;

public class MySQLServer extends AsyncServer<MySQLServerConnection> {

    public static final String DATABASE_NAME = "mysql";
    public static final int DEFAULT_PORT = 3306;

    @Override
    public String getType() {
        return MySQLServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);

        // 创建默认的mysql数据库
        createDefaultDatabase(DATABASE_NAME, true);
        // 以下三个默认数据库不需要持久化
        createDefaultDatabase("information_schema", false);
        createDefaultDatabase("performance_schema", false);
        createDefaultDatabase("sys", false);
    }

    private void createDefaultDatabase(String dbName, boolean persistent) {
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName //
                + " PARAMETERS(DEFAULT_SQL_ENGINE='" + MySQLServerEngine.NAME + "', PERSISTENT="
                + persistent + ")";
        LealoneDatabase.getInstance().getSystemSession().prepareStatementLocal(sql).executeUpdate();
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected MySQLServerConnection createConnection(WritableChannel writableChannel,
            Scheduler scheduler) {
        return new MySQLServerConnection(this, writableChannel, scheduler);
    }

    @Override
    protected void afterRegister(MySQLServerConnection conn, Scheduler scheduler) {
        int threadId = scheduler.getHandlerId();
        // 连接创建成功后先握手
        conn.handshake(threadId);
    }
}
