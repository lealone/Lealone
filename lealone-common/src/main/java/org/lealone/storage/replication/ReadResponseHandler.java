/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;

class ReadResponseHandler<T> extends ReplicationHandler<T> {

    ReadResponseHandler(ReplicationSession session, AsyncHandler<AsyncResult<T>> finalResultHandler) {
        super(session.r, finalResultHandler);
    }

    @Override
    void onSuccess() {
        if (finalResultHandler != null) {
            finalResultHandler.handle(results.get(0));
        }
    }
}
