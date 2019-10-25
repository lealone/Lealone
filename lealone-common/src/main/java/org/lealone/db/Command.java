/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db;

/**
 * Represents a command.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface Command extends AutoCloseable {

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
    @Override
    default void close() {
    }

    default void replicationCommit(long validKey, boolean autoCommit) {
    }

    default void replicationRollback() {
    }

    default int getId() {
        return 0;
    }

    default void setId(int id) {
    }
}
