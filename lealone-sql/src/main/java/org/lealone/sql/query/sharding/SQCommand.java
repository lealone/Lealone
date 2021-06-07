/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import java.util.List;

import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.sql.DistributedSQLCommand;
import org.lealone.storage.PageKey;

//Sharding Query Command
public class SQCommand {

    private final DistributedSQLCommand command;
    private final int maxRows;
    private final boolean scrollable;
    private final List<PageKey> pageKeys;

    public SQCommand(DistributedSQLCommand command, int maxRows, boolean scrollable, List<PageKey> pageKeys) {
        this.command = command;
        this.maxRows = maxRows;
        this.scrollable = scrollable;
        this.pageKeys = pageKeys;
    }

    public Future<Result> executeDistributedQuery() {
        return command.executeDistributedQuery(maxRows, scrollable, pageKeys);
    }
}
