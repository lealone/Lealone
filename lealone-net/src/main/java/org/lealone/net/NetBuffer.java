/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

public interface NetBuffer {

    NetBuffer appendBuffer(NetBuffer buff);

    int length();

    NetBuffer slice(int start, int end);

    NetBuffer getBuffer(int start, int end);

    short getUnsignedByte(int pos);

    NetBuffer appendByte(byte b);

    NetBuffer appendBytes(byte[] bytes, int offset, int len);

    NetBuffer appendInt(int i);

    NetBuffer setByte(int pos, byte b);

    NetBuffer flip();

    default boolean isOnlyOnePacket() {
        return false;
    }

    default void recycle() {
    }
}
