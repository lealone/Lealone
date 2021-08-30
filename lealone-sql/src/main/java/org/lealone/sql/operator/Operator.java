/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.operator;

import org.lealone.db.result.LocalResult;

public interface Operator {

    void start();

    void run();

    void stop();

    boolean isStopped();

    LocalResult getLocalResult();

}
