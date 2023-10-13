/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.docdb.server;

import java.util.Map;

import org.lealone.db.LealoneDatabase;
import org.lealone.net.WritableChannel;
import org.lealone.server.AsyncServer;
import org.lealone.server.Scheduler;

public class DocDBServer extends AsyncServer<DocDBServerConnection> {

    public static final String DATABASE_NAME = "docdb";
    public static final int DEFAULT_PORT = 9610;

    @Override
    public String getType() {
        return DocDBServerEngine.NAME;
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
    protected DocDBServerConnection createConnection(WritableChannel writableChannel,
            Scheduler scheduler) {
        return new DocDBServerConnection(this, writableChannel, scheduler, getConnectionSize() + 1);
    }
}
