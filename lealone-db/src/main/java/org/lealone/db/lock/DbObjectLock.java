/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.lock;

import org.lealone.db.DbObjectType;
import org.lealone.db.async.AsyncHandler;
import org.lealone.db.async.AsyncResult;
import org.lealone.db.session.ServerSession;

public interface DbObjectLock {

    DbObjectType getDbObjectType();

    void addHandler(AsyncHandler<AsyncResult<Boolean>> handler);

    boolean lock(ServerSession session, boolean exclusive);

    boolean trySharedLock(ServerSession session);

    boolean tryExclusiveLock(ServerSession session);

    void unlock(ServerSession session, boolean succeeded);

    boolean isLockedExclusively();

    boolean isLockedExclusivelyBy(ServerSession session);
}
