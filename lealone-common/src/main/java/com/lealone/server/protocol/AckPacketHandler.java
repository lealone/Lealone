/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol;

public interface AckPacketHandler<R, P extends AckPacket> {
    R handle(P ack);
}
