/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import java.util.ArrayList;

import com.lealone.db.DbObjectType;
import com.lealone.db.async.AsyncHandler;
import com.lealone.db.async.AsyncResult;
import com.lealone.db.session.ServerSession;
import com.lealone.db.session.Session;

//数据库对象模型已经支持多版本，所以对象锁只需要像行锁一样实现即可
public class DbObjectLock extends Lock {

    public static final RuntimeException LOCKED_EXCEPTION = new RuntimeException();

    private final DbObjectType type;
    private ArrayList<AsyncHandler<AsyncResult<Boolean>>> handlers;

    public DbObjectLock(DbObjectType type) {
        this.type = type;
    }

    @Override
    public String getLockType() {
        return type.name();
    }

    public boolean lock(ServerSession session, boolean exclusive) {
        if (exclusive)
            return tryExclusiveLock(session);
        else
            return trySharedLock(session);
    }

    public boolean trySharedLock(ServerSession session) {
        return true;
    }

    public boolean tryExclusiveLock(ServerSession session) {
        return tryLock(session.getTransaction(), this, null);
    }

    @Override
    public void unlock(Session oldSession, boolean succeeded, Session newSession) {
        if (handlers != null) {
            handlers.forEach(h -> {
                h.handle(new AsyncResult<>(succeeded));
            });
            handlers = null;
        }
        unlock(oldSession, newSession);
    }

    public void addHandler(AsyncHandler<AsyncResult<Boolean>> handler) {
        if (handlers == null)
            handlers = new ArrayList<>(1);
        handlers.add(handler);
    }
}
