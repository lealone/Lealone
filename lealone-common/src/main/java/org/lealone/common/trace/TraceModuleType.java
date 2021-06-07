/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.common.trace;

public enum TraceModuleType {

    /**
     * The trace module type for commands.
     */
    COMMAND,

    /**
     * The trace module type for constraints.
     */
    CONSTRAINT,

    /**
     * The trace module type for databases.
     */
    DATABASE,

    /**
     * The trace module type for functions.
     */
    FUNCTION,

    /**
     * The trace module type for indexes.
     */
    INDEX,

    /**
     * The trace module type for the JDBC API.
     */
    JDBC,

    /**
     * The trace module type for the JDBCX API
     */
    JDBCX,

    /**
     * The trace module type for locks.
     */
    LOCK,

    /**
     * The trace module type for schemas.
     */
    SCHEMA,

    /**
     * The trace module type for sequences.
     */
    SEQUENCE,

    /**
     * The trace module type for settings.
     */
    SETTING,

    /**
     * The trace module type for tables.
     */
    TABLE,

    /**
     * The trace module type for triggers.
     */
    TRIGGER,

    /**
     * The trace module type for users.
     */
    USER;
}
