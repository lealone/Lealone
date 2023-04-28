/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

public interface TransactionListener {

    default void beforeOperation() {
    }

    void operationUndo();

    void operationComplete();

    default void setException(RuntimeException e) {
    }

    default RuntimeException getException() {
        return null;
    }

    default void await() {
    }

    default void wakeUp() {
    }
}
