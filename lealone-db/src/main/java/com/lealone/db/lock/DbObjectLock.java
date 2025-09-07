/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.lock;

import java.util.ArrayList;

import com.lealone.db.DbObjectType;
import com.lealone.db.async.AsyncResultHandler;
import com.lealone.db.session.InternalSession;
import com.lealone.db.session.ServerSession;

//数据库对象模型已经支持多版本，所以对象锁只需要像行锁一样实现即可
public class DbObjectLock extends Lock {

    public static final RuntimeException LOCKED_EXCEPTION = new RuntimeException();

    private final DbObjectType type;
    private ArrayList<AsyncResultHandler<Boolean>> handlers;

    public DbObjectLock(DbObjectType type) {
        this.type = type;
    }

    @Override
    public String getLockType() {
        return type.name();
    }

    public boolean tryExclusiveLock(ServerSession session) {
        return tryLock(session.getTransaction(), this, null);
    }

    @Override
    public void unlock(InternalSession oldSession, boolean succeeded, InternalSession newSession) {
        if (handlers != null) {
            handlers.forEach(h -> {
                h.handleResult(succeeded);
            });
            handlers = null;
        }
        unlock(oldSession, newSession);
    }

    public void addHandler(AsyncResultHandler<Boolean> handler) {
        if (handlers == null)
            handlers = new ArrayList<>(1);
        handlers.add(handler);
    }
}
