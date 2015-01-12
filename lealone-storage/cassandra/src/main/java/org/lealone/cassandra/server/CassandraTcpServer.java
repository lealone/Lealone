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
package org.lealone.cassandra.server;

import java.net.Socket;
import java.sql.SQLException;
import java.util.ArrayList;

import org.lealone.cassandra.config.CassandraConfig;
import org.lealone.server.TcpServer;
import org.lealone.server.TcpServerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraTcpServer extends TcpServer {
    private static final Logger logger = LoggerFactory.getLogger(CassandraTcpServer.class);

    private final CassandraConfig config;

    public CassandraTcpServer(CassandraConfig config) {
        this.config = config;
        init();
    }

    @Override
    public void start() {
        try {
            super.start();
            logger.info("Lealone TcpServer started, listening address: {}, port: {}", getListenAddress(), getPort());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        super.stop();
        logger.info("Lealone TcpServer stopped");
    }

    @Override
    protected TcpServerThread createTcpServerThread(Socket socket, int threadId) {
        return new CassandraTcpServerThread(socket, this, threadId);
    }

    private void init() {
        ArrayList<String> args = new ArrayList<String>();
        args.add("-tcpListenAddress");
        args.add("" + config.listen_address);
        args.add("-tcpPort");
        args.add("" + config.lealone_tcp_port);
        super.init(args.toArray(new String[0]));
    }
}
