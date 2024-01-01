/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.net;

import com.lealone.db.DataBuffer;
import com.lealone.db.DataBufferFactory;

public interface NetBufferFactory {

    NetBuffer createBuffer(int initialSizeHint, DataBufferFactory dataBufferFactory);

    NetBuffer createBuffer(DataBuffer dataBuffer);

}
