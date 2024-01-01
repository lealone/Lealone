/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.plugins.mysql.server.handler;

import com.lealone.plugins.mysql.server.protocol.PacketInput;

public interface PacketHandler {

    void handle(PacketInput in);

}
