/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

public interface TransactionHandler {

    int getHandlerId();

    void addTransaction(PendingTransaction pt);

    PendingTransaction getTransaction();

    void wakeUp();

}
