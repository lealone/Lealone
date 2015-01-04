/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.server;

import java.sql.SQLException;

/**
 * Classes implementing this interface usually provide a
 * TCP/IP listener such as an FTP server.
 * The can be started and stopped, and may or may not
 * allow remote connections.
 */
public interface Server extends Runnable {

    /**
     * Initialize the server from command line options.
     *
     * @param args the command line options
     */
    void init(String... args);

    /**
     * Get the URL of this server in a human readable form
     *
     * @return the url
     */
    String getURL();

    /**
     * Start the server. 
     */
    void start() throws SQLException;

    /**
     * Stop the server.
     */
    void stop();

    /**
     * Check if the server is running.
     *
     * @param traceError if errors should be written
     * @return if the server is running
     */
    boolean isRunning(boolean traceError);

    /**
     * Check if remote connections are allowed.
     *
     * @return true if remote connections are allowed
     */
    boolean getAllowOthers();

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
    String getListenAddress();

    /**
     * Check if a daemon thread should be used.
     *
     * @return true if a daemon thread should be used
     */
    boolean isDaemon();

}
