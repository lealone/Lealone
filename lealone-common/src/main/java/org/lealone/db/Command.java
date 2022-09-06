/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db;

/**
 * Represents a command.
 *
 */
public interface Command {

    int CLIENT_SQL_COMMAND = -1;
    int CLIENT_PREPARED_SQL_COMMAND = -2;

    int CLIENT_STORAGE_COMMAND = -11;
    int SERVER_STORAGE_COMMAND = -12;

    int REPLICATION_SQL_COMMAND = -21;
    int REPLICATION_STORAGE_COMMAND = -22;

    /**
     * Get command type.
     *
     * @return the command type.
     */
    int getType();

    /**
     * Cancel the command if it is still processing.
     */
    default void cancel() {
    }

    /**
     * Close the command.
     */
    void close();
}
