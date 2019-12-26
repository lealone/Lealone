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
package org.lealone.client.session;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.session.DelegatedSession;
import org.lealone.db.session.Session;

class AutoReconnectSession extends DelegatedSession {

    private ConnectionInfo ci;
    private String newTargetNodes;

    public AutoReconnectSession(ConnectionInfo ci) {
        if (!ci.isRemote()) {
            throw DbException.throwInternalError();
        }
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

        ClientSessionFactory.getInstance().createSessionAsync(ci, ar -> {
            if (ar.isSucceeded()) {
                AutoReconnectSession a = (AutoReconnectSession) ar.getResult();
                session = a.session;
                oldSession.close();
                newTargetNodes = null;
            }
        });
    }

    @Override
    public void reconnectIfNeeded() {
        if (newTargetNodes != null) {
            reconnect();
        }
    }
}
