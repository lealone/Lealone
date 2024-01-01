/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.handler;

import com.lealone.db.session.ServerSession;
import com.lealone.server.protocol.Packet;
import com.lealone.server.scheduler.PacketHandleTask;

public interface PacketHandler<P extends Packet> {

    default Packet handle(ServerSession session, P packet) {
        return null;
    }

    default Packet handle(PacketHandleTask task, P packet) {
        return handle(task.session, packet);
    }
}
