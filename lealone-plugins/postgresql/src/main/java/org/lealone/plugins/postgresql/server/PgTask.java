/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.postgresql.server;

import org.lealone.net.NetBuffer;
import org.lealone.server.LinkableTask;

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
