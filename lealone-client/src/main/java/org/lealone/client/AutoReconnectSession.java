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
package org.lealone.client;

import java.net.InetSocketAddress;
import java.util.Random;

import org.lealone.common.concurrent.ConcurrentUtils;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.StringUtils;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.DelegatedSession;
import org.lealone.db.RunMode;
import org.lealone.db.Session;
import org.lealone.db.api.ErrorCode;
import org.lealone.net.NetEndpoint;
import org.lealone.replication.ReplicationSession;

class AutoReconnectSession extends DelegatedSession {

    private ConnectionInfo ci;
    private String newTargetEndpoints;

    public AutoReconnectSession(ConnectionInfo ci) {
        if (!ci.isRemote()) {
            throw DbException.throwInternalError();
        }
        this.ci = ci;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        super.setAutoCommit(autoCommit);

        if (newTargetEndpoints != null) {
            reconnect();
        }
    }

    @Override
    public Session connect() {
        return connect(true);
    }

    @Override
    public Session connect(boolean first) {
        InetSocketAddress inetSocketAddress = null;
        ClientSession clientSession = null;
        String[] servers = StringUtils.arraySplit(ci.getServers(), ',', true);
        Random random = new Random(System.currentTimeMillis());
        try {
            for (int i = 0, len = servers.length; i < len; i++) {
                String s = servers[random.nextInt(len)];
                try {
                    clientSession = new ClientSession(ci, s, this);
                    clientSession.connect();
                    inetSocketAddress = clientSession.getAsyncConnection().getInetSocketAddress();
                    session = clientSession;
                    break;
                } catch (Exception e) {
                    if (i == len - 1) {
                        throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, e, e + ": " + s);
                    }
                    int index = 0;
                    String[] newServers = new String[len - 1];
                    for (int j = 0; j < len; j++) {
                        if (j != i)
                            newServers[index++] = servers[j];
                    }
                    servers = newServers;
                    len--;
                    i = -1;
                }
            }
        } catch (DbException e) {
            throw e;
        }

        if (first) {
            if (getRunMode() == RunMode.REPLICATION) {
                ConnectionInfo ci = this.ci;
                servers = StringUtils.arraySplit(getTargetEndpoints(), ',', true);
                int size = servers.length;
                ClientSession[] sessions = new ClientSession[size];
                for (int i = 0; i < size; i++) {
                    // 如果首次连接的节点就是复制节点之一，则复用它
                    if (isValid()) {
                        NetEndpoint endpoint = NetEndpoint.createTCP(servers[i]);
                        if (endpoint.getInetSocketAddress().equals(inetSocketAddress)) {
                            sessions[i] = clientSession;
                            continue;
                        }
                    }
                    ci = this.ci.copy(servers[i]);
                    sessions[i] = new ClientSession(ci, servers[i], this);
                    sessions[i].connect(false);
                }
                ReplicationSession rs = new ReplicationSession(sessions);
                rs.setAutoCommit(this.isAutoCommit());
                session = rs;
                return this;
            }
            if (isInvalid()) {
                switch (getRunMode()) {
                case CLIENT_SERVER:
                case SHARDING: {
                    this.ci = this.ci.copy(getTargetEndpoints());
                    // 关闭当前session,因为连到的节点不是所要的,这里可能会关闭vertx,
                    // 所以要放在构造下一个ClientSession前调用
                    this.close();
                    return connect(false);
                }
                default:
                    throw DbException.throwInternalError();
                }
            }
        }
        return this;
    }

    @Override
    public void runModeChanged(String newTargetEndpoints) {
        this.newTargetEndpoints = newTargetEndpoints;
        if (session.isAutoCommit()) {
            reconnect();
        }
    }

    private void reconnect() {
        Session oldSession = this.session;
        this.ci = this.ci.copy(newTargetEndpoints);
        ConcurrentUtils.submitTask("Reconnect", () -> {
            connect();
            oldSession.close();
            newTargetEndpoints = null;
        });
    }

    @Override
    public void reconnectIfNeeded() {
        if (newTargetEndpoints != null) {
            reconnect();
        }
    }
}
