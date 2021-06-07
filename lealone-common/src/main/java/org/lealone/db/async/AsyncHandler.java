/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.async;

public interface AsyncHandler<E> {

    /**
     * Something has happened, so handle it.
     *
     * @param event the event to handle
     */
    void handle(E event);
}