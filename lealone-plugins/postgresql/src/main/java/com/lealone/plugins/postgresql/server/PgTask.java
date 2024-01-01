/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.postgresql.server;

import com.lealone.net.NetBuffer;
import com.lealone.server.scheduler.LinkableTask;

public class PgTask extends LinkableTask {

    public final PgServerConnection conn;
    public final NetBuffer buffer;

    public PgTask(PgServerConnection conn, NetBuffer buffer) {
        this.conn = conn;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        conn.handlePacket(buffer);
    }
}
