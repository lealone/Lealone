/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mongo.server;

import org.lealone.db.LealoneDatabase;
import org.lealone.db.scheduler.Scheduler;
import org.lealone.net.WritableChannel;
import org.lealone.server.AsyncServer;

public class MongoServer extends AsyncServer<MongoServerConnection> {

    public static final String DATABASE_NAME = "mongo";
    public static final int DEFAULT_PORT = 27017;

    @Override
    public String getType() {
        return MongoServerEngine.NAME;
    }

    @Override
    public synchronized void start() {
        super.start();

        // 创建默认的 mongodb 数据库
        String sql = "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME;
        LealoneDatabase.getInstance().getSystemSession().executeUpdateLocal(sql);
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected MongoServerConnection createConnection(WritableChannel writableChannel,
            Scheduler scheduler) {
        return new MongoServerConnection(this, writableChannel, scheduler, getConnectionSize() + 1);
    }
}
