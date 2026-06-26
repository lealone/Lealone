/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.server.http.container;

import java.io.File;

/**
 * A <code>Server</code> element represents the entire Catalina servlet container. Its attributes represent the
 * characteristics of the servlet container as a whole. A <code>Server</code> may contain one or more
 * <code>Services</code>, and the top level set of naming resources.
 * <p>
 * Normally, an implementation of this interface will also implement <code>Lifecycle</code>, such that when the
 * <code>start()</code> and <code>stop()</code> methods are called, all of the defined <code>Services</code> are also
 * started or stopped.
 * <p>
 * In between, the implementation must open a server socket on the port number specified by the <code>port</code>
 * property. When a connection is accepted, the first line is read and compared with the specified shutdown command. If
 * the command matches, shutdown of the server is initiated.
 */
public interface Server extends Lifecycle {

    // ------------------------------------------------------------- Properties

    File getBaseDir();

    void setBaseDir(File baseDir);

    // --------------------------------------------------------- Public Methods

    /**
     * Add a new Service to the set of defined Services.
     *
     * @param service The Service to be added
     */
    void addService(Service service);

    /**
     * Find the specified Service
     *
     * @param name Name of the Service to be returned
     *
     * @return the specified Service, or <code>null</code> if none exists.
     */
    Service findService(String name);

    /**
     * @return the array of Services defined within this Server.
     */
    Service[] findServices();

    /**
     * Remove the specified Service from the set associated from this Server.
     *
     * @param service The Service to be removed
     */
    void removeService(Service service);

}
