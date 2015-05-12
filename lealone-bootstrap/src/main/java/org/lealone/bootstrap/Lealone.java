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
package org.lealone.bootstrap;

import java.util.List;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.config.TransportServerOptions;
import org.lealone.cluster.config.TransportServerOptions.TcpServerOptions;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.router.P2PRouter;
import org.lealone.cluster.service.StorageService;
import org.lealone.cluster.utils.Utils;
import org.lealone.cluster.utils.WrappedRunnable;
import org.lealone.command.router.LocalRouter;
import org.lealone.command.router.Router;
import org.lealone.command.router.TransactionalRouter;
import org.lealone.engine.Constants;
import org.lealone.engine.DatabaseEngine;
import org.lealone.engine.Session;
import org.lealone.engine.SysProperties;
import org.lealone.server.PgServer;
import org.lealone.server.Server;
import org.lealone.server.TcpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lealone {
    private static final Logger logger = LoggerFactory.getLogger(Lealone.class);
    private static Config config;

    public static void main(String[] args) {
        start();
    }

    public static void start() {
        try {
            logger.info("Lealone version: {}", Utils.getReleaseVersionString());

            config = DatabaseDescriptor.loadConfig();

            logger.info("Run mode: {}", config.run_mode);

            if (!DatabaseDescriptor.hasLargeAddressSpace())
                logger.warn("32bit JVM detected. It is recommended to run lealone on a 64bit JVM for better performance.");

            initBaseDir();

            initDatabaseEngine();

            initRouter();

            if (config.isClusterMode())
                startClusterServer();

            startTcpServer();

            if (config.pg_server_options != null && config.pg_server_options.enabled)
                startPgServer();
        } catch (Exception e) {
            logger.error("Fatal error; unable to start Lealone.  See log for stacktrace.", e);
            System.exit(1);
        }
    }

    private static void initBaseDir() {
        if (config.base_dir == null)
            throw new ConfigurationException("base_dir must be specified");
        SysProperties.setBaseDir(config.base_dir);

        logger.info("Base dir: {}", config.base_dir);
    }

    private static void initDatabaseEngine() {
        String host = null;
        Integer port = null;

        if (config.tcp_server_options != null) {
            host = config.tcp_server_options.listen_address;
            port = config.tcp_server_options.port;
        }
        if (host == null)
            host = config.listen_address;
        if (host == null)
            host = Constants.DEFAULT_HOST;
        if (port == null)
            port = Constants.DEFAULT_TCP_PORT;

        DatabaseEngine.init(host, port.intValue());
    }

    private static void initRouter() {
        Router r = LocalRouter.getInstance();
        if (config.isClusterMode())
            r = P2PRouter.getInstance();
        Session.setRouter(new TransactionalRouter(r));
    }

    private static void startClusterServer() throws Exception {
        Session.setClusterMode(true);
        StorageService.instance.start();
    }

    private static void startTcpServer() throws Exception {
        if (config.tcp_server_options == null) {
            config.tcp_server_options = new TcpServerOptions();
            logger.warn("Use default TcpServer options");
        }
        startTransportServer(new TcpServer(), "Tcp", config.tcp_server_options);
    }

    private static void startPgServer() throws Exception {
        startTransportServer(new PgServer(), "Pg", config.pg_server_options);
    }

    private static void startTransportServer(final Server server, final String prefix, TransportServerOptions options)
            throws Exception {
        List<String> optionList = options.getOptions(config);
        logger.info(prefix + "Server options: {}", optionList);

        server.init(optionList.toArray(new String[0]));
        server.start();

        Thread t = new Thread(new WrappedRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                server.stop();
                logger.info(prefix + "Server stopped");
            }
        }, prefix + "ServerShutdownHook");
        Runtime.getRuntime().addShutdownHook(t);

        logger.info(prefix + "Server started, listening address: {}, port: {}", server.getListenAddress(),
                server.getPort());

    }
}
