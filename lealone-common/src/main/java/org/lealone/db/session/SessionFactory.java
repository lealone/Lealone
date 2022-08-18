/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.session;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.async.Future;

/**
 * A class that implements this interface can create new database sessions.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface SessionFactory {

    /**
     * Create a new session.
     *
     * @param ci the connection parameters
     * @return the new session
     */
    Future<Session> createSession(ConnectionInfo ci);

    default Future<Session> createSession(String url) {
        try {
            return createSession(new ConnectionInfo(url));
        } catch (Throwable t) {
            return Future.failedFuture(t);
        }
    }
}
