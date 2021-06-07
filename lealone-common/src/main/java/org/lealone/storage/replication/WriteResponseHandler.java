/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import java.util.ArrayList;
import java.util.List;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

class WriteResponseHandler<T> extends ReplicationHandler<T> {

    static interface ReplicationResultHandler<T> {
        T handleResults(List<T> results);
    }

    private final ReplicaCommand[] commands;
    private final ReplicationResultHandler<T> replicationResultHandler;
    private volatile boolean end;

    WriteResponseHandler(ReplicationSession session, ReplicaCommand[] commands,
            AsyncHandler<AsyncResult<T>> finalResultHandler) {
        this(session, commands, finalResultHandler, null);
    }

    WriteResponseHandler(ReplicationSession session, ReplicaCommand[] commands,
            AsyncHandler<AsyncResult<T>> finalResultHandler, ReplicationResultHandler<T> replicationResultHandler) {
        super(session.w, finalResultHandler);

        // 手动提交事务的场景不用执行副本提交
        if (!session.isAutoCommit())
            commands = null;
        this.commands = commands;
        this.replicationResultHandler = replicationResultHandler;
    }

    @Override
    void onSuccess() {
        AsyncResult<T> ar = null;
        if (replicationResultHandler != null) {
            T ret = replicationResultHandler.handleResults(getResults());
            ar = new AsyncResult<>(ret);
        } else {
            ar = results.get(0);
        }
        if (commands != null) {
            for (ReplicaCommand c : commands) {
                c.handleReplicaConflict(null);
            }
        }
        if (!end) {
            end = ar != null && ar.getResult() != null;
            if (finalResultHandler != null)
                finalResultHandler.handle(ar);
        }
    }

    private ArrayList<T> getResults() {
        int size = results.size();
        ArrayList<T> results2 = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
            results2.add(results.get(i).getResult());
        return results2;
    }
}
