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
import java.util.Map;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.config.PluggableEngineDef;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.router.P2PRouter;
import org.lealone.cluster.service.StorageService;
import org.lealone.cluster.utils.Utils;
import org.lealone.cluster.utils.WrappedRunnable;
import org.lealone.db.Constants;
import org.lealone.db.DatabaseEngine;
import org.lealone.db.PluggableEngine;
import org.lealone.db.Session;
import org.lealone.db.SysProperties;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.server.ProtocolServerEngineManager;
import org.lealone.sql.RouterHolder;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.router.LocalRouter;
import org.lealone.sql.router.Router;
import org.lealone.sql.router.TransactionalRouter;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageEngineManager;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lealone {
    private static final Logger logger = LoggerFactory.getLogger(Lealone.class);
    private static Config config;

    public static void main(String[] args) {
        logger.info("Lealone version: {}", Utils.getReleaseVersionString());
        try {
            init();
            start();
        } catch (Exception e) {
            logger.error("Fatal error: unable to start lealone. See log for stacktrace.", e);
            System.exit(1);
        }
    }

    private static void init() {
        config = DatabaseDescriptor.loadConfig();

        logger.info("Run mode: {}", config.run_mode);

        if (!DatabaseDescriptor.hasLargeAddressSpace())
            logger.warn("32bit JVM detected. It is recommended to run lealone on a 64bit JVM for better performance.");

        initBaseDir();
        initPluggableEngines();
        initDatabaseEngine();
        initRouter();
    }

    private static void initBaseDir() {
        if (config.base_dir == null || config.base_dir.isEmpty())
            throw new ConfigurationException("base_dir must be specified and not empty");
        SysProperties.setBaseDir(config.base_dir);

        logger.info("Base dir: {}", config.base_dir);
    }

    private static void initDatabaseEngine() {
        String host = config.listen_address;
        Integer port = config.listen_port;

        if (host == null)
            config.listen_address = host = Constants.DEFAULT_HOST;
        if (port == null)
            config.listen_port = port = Constants.DEFAULT_TCP_PORT;

        DatabaseEngine.init(host, port.intValue());
    }

    // 初始化顺序storage -> transaction -> sql -> protocol
    private static void initPluggableEngines() {
        List<PluggableEngineDef> pluggable_engines = config.storage_engines;
        if (pluggable_engines != null) {
            for (PluggableEngineDef def : pluggable_engines) {
                if (def.enabled) {
                    StorageEngine se = StorageEngineManager.getInstance().getEngine(def.name);
                    if (se == null) {
                        try {
                            Class<?> clz = Utils.loadUserClass(def.name);
                            se = (StorageEngine) clz.newInstance();
                            StorageEngineManager.getInstance().registerEngine(se, clz.getName());
                        } catch (Exception e) {
                            throw new ConfigurationException("StorageEngine '" + def.name + "' can not found");
                        }
                    }
                    initPluggableEngine(se, def);
                }
            }
        }
        pluggable_engines = config.transaction_engines;
        if (pluggable_engines != null) {
            for (PluggableEngineDef def : pluggable_engines) {
                if (def.enabled) {
                    TransactionEngine te = TransactionEngineManager.getInstance().getEngine(def.name);
                    if (te == null) {
                        try {
                            Class<?> clz = Utils.loadUserClass(def.name);
                            te = (TransactionEngine) clz.newInstance();
                            TransactionEngineManager.getInstance().registerEngine(te, clz.getName());
                        } catch (Exception e) {
                            throw new ConfigurationException("TransactionEngine '" + def.name + "' can not found");
                        }
                    }
                    if (config.isClusterMode())
                        def.getParameters().put("is_cluster_mode", "true");
                    initPluggableEngine(te, def);
                }
            }
        }
        pluggable_engines = config.sql_engines;
        if (pluggable_engines != null) {
            for (PluggableEngineDef def : pluggable_engines) {
                if (def.enabled) {
                    SQLEngine se = SQLEngineManager.getInstance().getEngine(def.name);
                    if (se == null) {
                        try {
                            Class<?> clz = Utils.loadUserClass(def.name);
                            se = (SQLEngine) clz.newInstance();
                            SQLEngineManager.getInstance().registerEngine(se, clz.getName());
                        } catch (Exception e) {
                            throw new ConfigurationException("SQLEngine '" + def.name + "' can not found");
                        }
                    }
                    initPluggableEngine(se, def);
                }
            }
        }

        pluggable_engines = config.protocol_server_engines;
        if (pluggable_engines != null) {
            for (PluggableEngineDef def : pluggable_engines) {
                if (def.enabled) {
                    ProtocolServerEngine pse = ProtocolServerEngineManager.getInstance().getEngine(def.name);
                    if (pse == null) {
                        try {
                            Class<?> clz = Utils.loadUserClass(def.name);
                            pse = (ProtocolServerEngine) clz.newInstance();
                            ProtocolServerEngineManager.getInstance().registerEngine(pse, clz.getName());
                        } catch (Exception e) {
                            throw new ConfigurationException("ProtocolServerEngine '" + def.name + "' can not found");
                        }
                    }
                    initPluggableEngine(pse, def);
                }
            }
        }
    }

    private static void initPluggableEngine(PluggableEngine pe, PluggableEngineDef def) {
        if (!def.getParameters().containsKey("base_dir"))
            def.getParameters().put("base_dir", config.base_dir);

        pe.init(def.getParameters());
    }

    private static void initRouter() {
        Router r = LocalRouter.getInstance();
        if (config.isClusterMode())
            r = P2PRouter.getInstance();
        RouterHolder.setRouter(new TransactionalRouter(r));
    }

    private static void start() throws Exception {
        if (config.isClusterMode())
            startClusterServer();

        startProtocolServers();
    }

    private static void startClusterServer() throws Exception {
        Session.setClusterMode(true);
        StorageService.instance.start();
    }

    private static void startProtocolServers() throws Exception {
        List<PluggableEngineDef> protocol_server_engines = config.protocol_server_engines;
        if (protocol_server_engines != null) {
            for (PluggableEngineDef def : protocol_server_engines) {
                if (def.enabled) {
                    ProtocolServerEngine pse = ProtocolServerEngineManager.getInstance().getEngine(def.name);
                    ProtocolServer protocolServer = pse.getProtocolServer();
                    startProtocolServer(protocolServer, def.getParameters());
                }
            }
        }
    }

    private static void startProtocolServer(final ProtocolServer server, Map<String, String> parameters)
            throws Exception {
        if (!parameters.containsKey("listen_address"))
            parameters.put("listen_address", config.listen_address);
        if (!parameters.containsKey("port"))
            parameters.put("port", config.listen_port.toString());

        final String name = server.getName();
        server.init(parameters);
        server.start();

        Thread t = new Thread(new WrappedRunnable() {
            @Override
            public void runMayThrow() throws Exception {
                server.stop();
                logger.info(name + " stopped");
            }
        }, name + "ShutdownHook");
        Runtime.getRuntime().addShutdownHook(t);

        logger.info(name + " started, listen address: {}, port: {}", server.getListenAddress(), server.getPort());
    }
}
