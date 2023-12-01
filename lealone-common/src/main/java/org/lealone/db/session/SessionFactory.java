/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.session;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.Plugin;
import org.lealone.db.async.Future;

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
