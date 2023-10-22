/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.plugins.mysql.server.handler;

import org.lealone.plugins.mysql.server.protocol.PacketInput;

public interface PacketHandler {

    void handle(PacketInput in);

}
