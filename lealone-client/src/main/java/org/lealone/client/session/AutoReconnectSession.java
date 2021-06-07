/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.client.session;

import org.lealone.db.ConnectionInfo;
import org.lealone.db.session.DelegatedSession;
import org.lealone.db.session.Session;

class AutoReconnectSession extends DelegatedSession {

    private ConnectionInfo ci;
    private String newTargetNodes;

    AutoReconnectSession(ConnectionInfo ci) {
        this.ci = ci;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        super.setAutoCommit(autoCommit);

        if (newTargetNodes != null) {
            reconnect();
        }
    }

    @Override
    public void runModeChanged(String newTargetNodes) {
        this.newTargetNodes = newTargetNodes;
        if (session.isAutoCommit()) {
            reconnect();
        }
    }

    private void reconnect() {
        Session oldSession = this.session;
        this.ci = this.ci.copy(newTargetNodes);

        ClientSessionFactory.getInstance().createSession(ci).onSuccess(s -> {
            AutoReconnectSession a = (AutoReconnectSession) s;
            session = a.session;
            oldSession.close();
            newTargetNodes = null;
        });
    }

    @Override
    public void reconnectIfNeeded() {
        if (newTargetNodes != null) {
            reconnect();
        }
    }
}
