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

import org.lealone.common.exceptions.DbException;
import org.lealone.server.ProtocolServerBase;

public abstract class NetServerBase extends ProtocolServerBase implements NetServer {

    protected AsyncConnectionManager connectionManager;

    @Override
    public void setConnectionManager(AsyncConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    private void check() {
        if (connectionManager == null)
            throw DbException.throwInternalError("connectionManager is null");
    }

    public AsyncConnection createConnection(WritableChannel writableChannel, boolean isServer) {
        check();
        return connectionManager.createConnection(writableChannel, isServer);

    }

    public void removeConnection(AsyncConnection conn) {
        check();
        connectionManager.removeConnection(conn);
    }

}
