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
package org.lealone.hbase.server;

import java.net.Socket;

import org.lealone.engine.ConnectionInfo;
import org.lealone.hbase.engine.HBaseConnectionInfo;
import org.lealone.server.TcpServerThread;

public class HBaseTcpServerThread extends TcpServerThread {
    private final HBaseTcpServer server;

    protected HBaseTcpServerThread(Socket socket, HBaseTcpServer server, int threadId) {
        super(socket, server, threadId);
        this.server = server;
    }

    @Override
    protected ConnectionInfo createConnectionInfo(String dbName, String originalURL) {
        ConnectionInfo ci = new HBaseConnectionInfo(server, originalURL, dbName);
        if (server.getMaster() != null)
            ci.setProperty("SERVER_TYPE", "M");
        else if (server.getRegionServer() != null)
            ci.setProperty("SERVER_TYPE", "RS");
        return ci;
    }
}
