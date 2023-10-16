/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.mysql.server.handler;

import org.lealone.mysql.server.protocol.PacketInput;

public interface PacketHandler {

    void handle(PacketInput in);

}
