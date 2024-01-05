/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.cassandra.server;

import com.lealone.server.ProtocolServer;
import com.lealone.server.ProtocolServerEngineBase;

public class CassandraServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "Cassandra";

    public CassandraServerEngine() {
        super(NAME);
    }

    @Override
    protected ProtocolServer createProtocolServer() {
        return new CassandraServer();
    }
}
