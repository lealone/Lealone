/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.session;

public enum SessionStatus {

    TRANSACTION_NOT_START,
    TRANSACTION_NOT_COMMIT,
    TRANSACTION_COMMITTING,

    WAITING,
    STATEMENT_RUNNING,
    STATEMENT_COMPLETED,

    EXCLUSIVE_MODE

}
