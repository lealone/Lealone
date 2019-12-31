/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.session;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.async.Future;

/**
 * A class that implements this interface can create new database sessions.
 */
public interface SessionFactory {

    /**
     * Create a new session.
     *
     * @param ci the connection parameters
     * @return the new session
     */
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
