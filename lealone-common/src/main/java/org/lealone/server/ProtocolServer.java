/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import java.util.Map;

import org.lealone.common.security.EncryptionOptions.ServerEncryptionOptions;
import org.lealone.db.scheduler.Scheduler;

/**
 * Classes implementing this interface usually provide a TCP/IP listener such as an TCP server.
 * The server can be started and stopped, and may or may not allow remote connections.
 * 
 * @author H2 Group
 * @author zhh
 */
public interface ProtocolServer {

    /**
     * Initialize the server
     *
     * @param config the config
     */
    void init(Map<String, String> config);

    /**
     * Start the server. 
     */
    void start();

    /**
     * Stop the server.
     */
    void stop();

    boolean isStarted();

    boolean isStopped();

    /**
     * Get the URL of this server in a human readable form
     *
     * @return the url
     */
    String getURL();

    /**
     * Gets the port this server is listening on.
     *
     * @return the port
     */
    int getPort();

    /**
     * Gets the address this server is listening on.
     *
     * @return the address
     */
    String getHost();

    /**
     * Get the human readable name of the server.
     *
     * @return the name
     */
    String getName();

    /**
     * Get the human readable short name of the server.
     *
     * @return the type
     */
    String getType();

    /**
     * Check if remote connections are allowed.
     *
     * @return true if remote connections are allowed
     */
    boolean getAllowOthers();

    /**
     * Check if a daemon thread should be used.
     *
     * @return true if a daemon thread should be used
     */
    boolean isDaemon();

    void setServerEncryptionOptions(ServerEncryptionOptions options);

    ServerEncryptionOptions getServerEncryptionOptions();

    boolean isSSL();

    Map<String, String> getConfig();

    /**
     * Get the configured base directory.
     *
     * @return the base directory
     */
    String getBaseDir();

    /**
     * Check if this socket may connect to this server.
     * Remote connections are allowed if the flag allowOthers is set.
     *
     * @param socket the socket
     * @return true if this client may connect
     */
    boolean allow(String testHost);

    default int getSessionTimeout() {
        return -1;
    }

    default void accept(Scheduler scheduler) {
    }
}
