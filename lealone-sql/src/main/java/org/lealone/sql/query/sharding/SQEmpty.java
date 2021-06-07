/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import org.lealone.db.result.LocalResult;

//返回一个空结果
public class SQEmpty extends SQOperator {

    public SQEmpty() {
        super(null, 0);
    }

    @Override
    public void run() {
        result = new LocalResult();
        end = true;
    }
}
