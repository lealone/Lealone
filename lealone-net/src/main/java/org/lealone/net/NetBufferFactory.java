/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

import org.lealone.db.DataBuffer;
import org.lealone.db.DataBufferFactory;

public interface NetBufferFactory {

    NetBuffer createBuffer(int initialSizeHint, DataBufferFactory dataBufferFactory);

    NetBuffer createBuffer(DataBuffer dataBuffer);

}
