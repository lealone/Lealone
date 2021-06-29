/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql;

import java.util.List;

import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.storage.PageKey;

public interface DistributedSQLCommand extends SQLCommand {

    Future<Result> executeDistributedQuery(int maxRows, boolean scrollable, List<PageKey> pageKeys, String indexName);

    Future<Integer> executeDistributedUpdate(List<PageKey> pageKeys, String indexName);

}
