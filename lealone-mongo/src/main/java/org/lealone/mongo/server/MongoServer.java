/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mongo.server;

import java.util.Map;

import org.lealone.db.LealoneDatabase;
import org.lealone.net.WritableChannel;
import org.lealone.server.AsyncServer;
import org.lealone.server.Scheduler;

public class MongoServer extends AsyncServer<MongoServerConnection> {

    public static final String DATABASE_NAME = "mongo";
    public static final int DEFAULT_PORT = 9610;

    @Override
    public String getType() {
        return MongoServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);

        // 创建默认的 mongodb 数据库
        String sql = "CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME;
        LealoneDatabase.getInstance().getSystemSession().prepareStatementLocal(sql).executeUpdate();
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
