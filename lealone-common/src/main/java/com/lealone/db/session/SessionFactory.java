/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.session;

import com.lealone.db.ConnectionInfo;
import com.lealone.db.Plugin;
import com.lealone.db.async.Future;

public interface SessionFactory extends Plugin {

    default Future<Session> createSession(ConnectionInfo ci) {
        return createSession(ci, true);
    }

    default Future<Session> createSession(String url) {
        try {
            return createSession(new ConnectionInfo(url), true);
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }

    Future<Session> createSession(ConnectionInfo ci, boolean allowRedirect);
}
