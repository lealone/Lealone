/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

public interface NetBuffer {

    int length();

    short getUnsignedByte(int pos);

    void read(byte[] dst, int off, int len);

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
