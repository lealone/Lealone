/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.query.sharding;

import org.lealone.db.async.Future;
import org.lealone.db.result.Result;
import org.lealone.server.protocol.dt.DTransactionParameters;
import org.lealone.sql.DistributedSQLCommand;

//Sharding Query Command
public class SQCommand {

    private final DistributedSQLCommand command;
    private final int maxRows;
    private final boolean scrollable;
    private final DTransactionParameters parameters;

    public SQCommand(DistributedSQLCommand command, int maxRows, boolean scrollable,
            DTransactionParameters parameters) {
        this.command = command;
        this.maxRows = maxRows;
        this.scrollable = scrollable;
        this.parameters = parameters;
    }

    public Future<Result> executeDistributedQuery() {
        return command.executeDistributedQuery(maxRows, scrollable, parameters);
    }
}
