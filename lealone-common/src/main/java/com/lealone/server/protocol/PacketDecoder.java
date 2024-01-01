/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.server.protocol;

import java.io.IOException;

import com.lealone.net.NetInputStream;

public interface PacketDecoder<T extends Packet> {

    T decode(NetInputStream in, int version) throws IOException;

}
