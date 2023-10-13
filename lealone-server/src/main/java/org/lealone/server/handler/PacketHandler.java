/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.handler;

import org.lealone.db.session.ServerSession;
import org.lealone.server.PacketHandleTask;
import org.lealone.server.protocol.Packet;

public interface PacketHandler<P extends Packet> {

    default Packet handle(ServerSession session, P packet) {
        return null;
    }

    default Packet handle(PacketHandleTask task, P packet) {
        return handle(task.session, packet);
    }
}
