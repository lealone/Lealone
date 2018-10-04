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

import java.net.BindException;

import org.lealone.common.exceptions.ConfigException;
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

    protected void checkBindException(Throwable e, String message) {
        String address = host + ":" + port;
        if (e instanceof BindException) {
            if (e.getMessage().contains("in use")) {
                message += ", " + address + " is in use by another process. Change host:port in lealone.yaml "
                        + "to values that do not conflict with other services.";
                e = null;
            } else if (e.getMessage().contains("Cannot assign requested address")) {
                message += ". Unable to bind to address " + address
                        + ". Set host:port in lealone.yaml to an interface you can bind to, e.g.,"
                        + " your private IP address.";
                e = null;
            }
        }
        if (e == null)
            throw new ConfigException(message);
        else
            throw new ConfigException(message, e);
    }
}
