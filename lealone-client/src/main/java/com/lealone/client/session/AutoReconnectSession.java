/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.client.session;

import com.lealone.db.ConnectionInfo;
import com.lealone.db.session.DelegatedSession;
import com.lealone.db.session.Session;

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
        ci.getSessionFactory().createSession(ci).onSuccess(s -> {
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
