/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.lealone.db.value.Value;

public interface NetInputStream {

    int readInt() throws IOException;

    long readLong() throws IOException;

    String readString() throws IOException;

    ByteBuffer readByteBuffer() throws IOException;

    boolean readBoolean() throws IOException;

    byte[] readBytes() throws IOException;

    void readBytes(byte[] buff, int off, int len) throws IOException;

    Value readValue() throws IOException;

}
