/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.cassandra.server;

import java.util.Map;

import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.net.WritableChannel;
import com.lealone.server.AsyncServer;

public class CassandraServer extends AsyncServer<CassandraServerConnection> {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CassandraServer.class);

    public static final String VERSION = "5.0";
    public static final int DEFAULT_PORT = 9042;

    @Override
    public String getType() {
        return CassandraServerEngine.NAME;
    }

    @Override
    public void init(Map<String, String> config) {
        super.init(config);
    }

    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    protected int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    protected CassandraServerConnection createConnection(WritableChannel writableChannel,
            Scheduler scheduler) {
        return new CassandraServerConnection(this, writableChannel, scheduler);
    }

    @Override
    protected void beforeRegister(CassandraServerConnection conn, Scheduler scheduler) {
    }
}
