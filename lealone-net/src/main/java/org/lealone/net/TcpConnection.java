/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.net;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Session;

/**
 * An async tcp connection.
 */
public abstract class TcpConnection extends TransferConnection {

    private static final Logger logger = LoggerFactory.getLogger(TcpConnection.class);

    private final ConcurrentHashMap<Integer, Session> sessions = new ConcurrentHashMap<>();

    public TcpConnection(WritableChannel writableChannel, boolean isServer) {
        super(writableChannel, isServer);
    }

    public void addSession(int sessionId, Session session) {
        sessions.put(sessionId, session);
    }

    public Session removeSession(int sessionId) {
        return sessions.remove(sessionId);
    }

    public Session getSession(int sessionId) {
        return sessions.get(sessionId);
    }

    public Collection<Session> getSessions() {
        return sessions.values();
    }

    protected void closeSession(Session session) {
        if (session != null) {
            try {
                session.prepareStatement("ROLLBACK", -1).executeUpdate();
                session.close();
            } catch (Exception e) {
                logger.error("Failed to close session", e);
            }
        }
    }

    /**
     * Close a connection.
     */
    @Override
    public void close() {
        try {
            for (Session s : sessions.values())
                closeSession(s);
            sessions.clear();
            super.close();
        } catch (Exception e) {
            logger.error("Failed to close connection", e);
        }
    }
}
