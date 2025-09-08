/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

public enum SessionStatus {

    TRANSACTION_NOT_START,
    TRANSACTION_NOT_COMMIT,
    TRANSACTION_COMMITTING,

    WAITING,
    RETRYING,
    RETRYING_RETURN_ACK,

    STATEMENT_RUNNING,
    STATEMENT_YIELDED,
    STATEMENT_COMPLETED

}
