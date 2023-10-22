/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server;

import java.util.Map;

import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngineBase;

public class PgServerEngine extends ProtocolServerEngineBase {

    public static final String NAME = "PostgreSQL";
    private final PgServer pgServer = new PgServer();

    public PgServerEngine() {
        super(NAME);
    }

    @Override
    public ProtocolServer getProtocolServer() {
        return pgServer;
    }

    @Override
    public void init(Map<String, String> config) {
        pgServer.init(config);
    }

    @Override
    public void close() {
        pgServer.stop();
    }
}
