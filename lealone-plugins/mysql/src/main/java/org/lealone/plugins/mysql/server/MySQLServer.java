/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.server;

import org.lealone.db.Database;
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
    public synchronized void start() {
        super.start();

        // 创建内置的mysql数据库
        createBuiltInDatabase(DATABASE_NAME);

        // mysql数据库内置了5个schema：information_schema、performance_schema、public、mysql、sys
        // 前三个在执行create database时自动创建了，这里需要再创建mysql、sys
        createBuiltInSchemas(DATABASE_NAME);
    }

    private void createBuiltInDatabase(String dbName) {
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName //
                + " PARAMETERS(DEFAULT_SQL_ENGINE='" + MySQLServerEngine.NAME //
                + "', MODE='" + MySQLServerEngine.NAME + "')";
        LealoneDatabase.getInstance().getSystemSession().prepareStatementLocal(sql).executeUpdate();
    }

    public static void createBuiltInSchemas(String dbName) {
        createBuiltInSchema(dbName, DATABASE_NAME);
        createBuiltInSchema(dbName, "sys");
    }

    private static void createBuiltInSchema(String dbName, String schemaName) {
        Database db = LealoneDatabase.getInstance().getDatabase(dbName);
        if (!db.isInitialized())
            db.init();
        String sql = "CREATE SCHEMA IF NOT EXISTS " + schemaName + " AUTHORIZATION root";
        db.getSystemSession().prepareStatementLocal(sql).executeUpdate();
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
