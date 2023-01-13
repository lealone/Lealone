/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction;

import org.lealone.db.session.Session;

public interface TransactionListener {

    default void beforeOperation() {
    }

    void operationUndo();

    void operationComplete();

    default void setException(RuntimeException e) {
    }

    default void setException(Throwable t) {
        setException(new RuntimeException(t));
    }

    default RuntimeException getException() {
        return null;
    }

    default void await() {
    }

    default void wakeUp() {
    }

    default Object addSession(Session session, Object parentSessionInfo) {
        return null;
    }

    default void removeSession(Object sessionInfo) {
    }
}
