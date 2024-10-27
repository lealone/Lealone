/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.lealone.db.value.Value;
import com.lealone.storage.page.PageKey;

public interface NetOutputStream {

    void setSSL(boolean ssl);

    NetOutputStream writeInt(int x) throws IOException;

    NetOutputStream writeLong(long x) throws IOException;

    NetOutputStream writeString(String s) throws IOException;

    NetOutputStream writeByteBuffer(ByteBuffer data) throws IOException;

    NetOutputStream writePageKey(PageKey pk) throws IOException;

    NetOutputStream writeBytes(byte[] data) throws IOException;

    NetOutputStream writeBoolean(boolean x) throws IOException;

    void writeValue(Value v) throws IOException;

}
