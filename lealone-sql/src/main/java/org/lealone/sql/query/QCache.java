/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query;

import org.lealone.db.result.LocalResult;

// 直接基于缓存中的结果集
class QCache extends QOperator {

    QCache(Select select, LocalResult result) {
        super(select);
        this.result = result;
        localResult = result;
    }

    @Override
    void run() {
        loopEnd = true;
    }
}
