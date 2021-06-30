/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.server.protocol.dt.DTransactionParameters;

public interface DistributedSQLCommand extends SQLCommand {

    Future<Result> executeDistributedQuery(int maxRows, boolean scrollable, DTransactionParameters parameters);

    Future<Integer> executeDistributedUpdate(DTransactionParameters parameters);

}
