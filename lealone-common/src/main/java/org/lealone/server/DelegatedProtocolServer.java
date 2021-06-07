/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.server;

import java.util.Map;

import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;

public class DelegatedProtocolServer implements ProtocolServer {

    protected ProtocolServer protocolServer;

    public ProtocolServer getProtocolServer() {
        return protocolServer;
    }

    public void setProtocolServer(ProtocolServer protocolServer) {
        this.protocolServer = protocolServer;
    }

    @Override
    public void init(Map<String, String> config) {
        protocolServer.init(config);
    }

    @Override
    public void start() {
        protocolServer.start();
    }

    @Override
    public void stop() {
        protocolServer.stop();
    }

    @Override
    public boolean isRunning(boolean traceError) {
        return protocolServer.isRunning(traceError);
    }

    @Override
    public String getURL() {
        return protocolServer.getURL();
    }

    @Override
    public int getPort() {
        return protocolServer.getPort();
    }

    @Override
    public String getHost() {
        return protocolServer.getHost();
    }

    @Override
    public String getName() {
        return protocolServer.getName();
    }

    @Override
    public String getType() {
        return protocolServer.getType();
    }

    @Override
    public boolean getAllowOthers() {
        return protocolServer.getAllowOthers();
    }

    @Override
    public boolean isDaemon() {
        return protocolServer.isDaemon();
    }

    @Override
    public void setServerEncryptionOptions(ServerEncryptionOptions options) {
        protocolServer.setServerEncryptionOptions(options);
    }

    @Override
    public ServerEncryptionOptions getServerEncryptionOptions() {
        return protocolServer.getServerEncryptionOptions();
    }

    @Override
    public boolean isSSL() {
        return protocolServer.isSSL();
    }

    @Override
    public Map<String, String> getConfig() {
        return protocolServer.getConfig();
    }

    @Override
    public String getBaseDir() {
        return protocolServer.getBaseDir();
    }

    @Override
    public boolean isStarted() {
        if (protocolServer == null)
            return false;
        return protocolServer.isStarted();
    }

    @Override
    public boolean isStopped() {
        return protocolServer.isStopped();
    }

    @Override
    public boolean allow(String testHost) {
        return protocolServer.allow(testHost);
    }

    @Override
    public Runnable getRunnable() {
        return protocolServer.getRunnable();
    }

    @Override
    public int getSessionTimeout() {
        return protocolServer.getSessionTimeout();
    }
}
