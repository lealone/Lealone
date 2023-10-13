/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
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
            throw DbException.getInternalError("connectionManager is null");
    }

    public AsyncConnection createConnection(WritableChannel writableChannel, Object scheduler) {
        check();
        return connectionManager.createConnection(writableChannel, true, scheduler);
    }

    public void removeConnection(AsyncConnection conn) {
        check();
        connectionManager.removeConnection(conn);
    }

    protected void checkBindException(Throwable e, String message) {
        String address = host + ":" + port;
        if (e instanceof BindException) {
            if (e.getMessage().contains("in use")) {
                message += ", " + address
                        + " is in use by another process. Change host:port in lealone.yaml "
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
