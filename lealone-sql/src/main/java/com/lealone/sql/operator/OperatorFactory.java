/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.sql.operator;

import com.lealone.db.Plugin;
import com.lealone.db.result.LocalResult;
import com.lealone.sql.query.Select;

public interface OperatorFactory extends Plugin {

    Operator createOperator(Select select);

    default Operator createOperator(Select select, LocalResult localResult) {
        return createOperator(select);
    }

}
