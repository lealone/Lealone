/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.server;

import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.PluginManager;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.WritableChannel;
import com.lealone.plugins.mysql.sql.expression.function.MySQLFunctionFactory;
import com.lealone.server.AsyncServer;
import com.lealone.storage.StorageEngine;

public class MySQLServer extends AsyncServer<MySQLServerConnection> {

    public static final String DATABASE_NAME = "mysql";
    public static final int DEFAULT_PORT = 3306;
    public static final String SERVER_VERSION = "5.7.35";

    @Override
    public String getType() {
        return MySQLServerEngine.NAME;
    }

    @Override
    public synchronized void start() {
        super.start();

        // 识别 "InnoDB"
        PluginManager.register(StorageEngine.class, StorageEngine.getDefaultStorageEngine(), "InnoDB");

        // 创建内置的mysql数据库
        createBuiltInDatabase(DATABASE_NAME);

        // mysql数据库内置了5个schema：information_schema、performance_schema、public、mysql、sys
        // 前三个在执行create database时自动创建了，这里需要再创建mysql、sys
        createBuiltInSchemas(DATABASE_NAME);

        // 注册内置函数工厂
        MySQLFunctionFactory.register();
    }

    private void createBuiltInDatabase(String dbName) {
        String sql = "CREATE DATABASE IF NOT EXISTS " + dbName //
                + " PARAMETERS(DEFAULT_SQL_ENGINE='" + MySQLServerEngine.NAME //
                + "', MODE='" + MySQLServerEngine.NAME + "')";
        LealoneDatabase.getInstance().getSystemSession().executeUpdateLocal(sql);
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
        db.getSystemSession().executeUpdateLocal(sql);
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
        int threadId = scheduler.getId();
        // 连接创建成功后先握手
        conn.handshake(threadId);
    }
}
