/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

// 比如 limit子句为0时
class QEmpty extends QOperator {

    QEmpty(Select select) {
        super(select);
    }

    @Override
    void run() {
        loopEnd = true;
    }
}
