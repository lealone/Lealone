/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

//有些命令不必非得调用close，避免加@SuppressWarnings("resource")
public interface ManualCloseable {
    void close();
}
