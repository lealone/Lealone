/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

public interface RedoLogSyncListener {

    // 没有用getId，考虑到实现类可能继承自java.lang.Thread，它里面也有一个getId，会导致冲突
    int getListenerId();

    void addWaitingTransaction(Transaction transaction);

    void wakeUpListener();
}
