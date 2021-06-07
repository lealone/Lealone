/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.net;

public interface NetBufferFactory {

    NetBuffer createBuffer(int initialSizeHint);

}
