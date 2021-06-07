/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server.protocol;

import java.io.IOException;

import org.lealone.net.NetOutputStream;

public interface Packet {

    PacketType getType();

    PacketType getAckType();

    void encode(NetOutputStream out, int version) throws IOException;
}
