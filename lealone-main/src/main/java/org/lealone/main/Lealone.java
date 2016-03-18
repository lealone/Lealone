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
package org.lealone.main;

import java.util.List;
import java.util.Map;

import org.lealone.common.exceptions.ConfigurationException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.Utils;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluggableEngine;
import org.lealone.db.SysProperties;
import org.lealone.main.config.Config;
import org.lealone.main.config.ConfigurationLoader;
import org.lealone.main.config.PluggableEngineDef;
import org.lealone.main.config.YamlConfigurationLoader;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.server.ProtocolServerEngineManager;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.router.Router;
import org.lealone.sql.router.RouterHolder;
import org.lealone.sql.router.TransactionalRouter;
import org.lealone.storage.StorageEngine;
import org.lealone.storage.StorageEngineManager;
import org.lealone.transaction.TransactionEngine;
import org.lealone.transaction.TransactionEngineManager;

public class Lealone {
    private static final Logger logger = LoggerFactory.getLogger(Lealone.class);
    private static Config config;

    public static void main(String[] args) {
        logger.info("Lealone version: {}", Utils.getReleaseVersionString());

        try {
            long t = System.currentTimeMillis();

            loadConfig();

            long t1 = (System.currentTimeMillis() - t);
            t = System.currentTimeMillis();

            init();

            long t2 = (System.currentTimeMillis() - t);
            t = System.currentTimeMillis();

            start();

            long t3 = (System.currentTimeMillis() - t);
            logger.info("Total time: {} ms (Load config: {} ms, Init: {} ms, Start: {} ms)", (t1 + t2 + t3), t1, t2, t3);
        } catch (Exception e) {
            logger.error("Fatal error: unable to start lealone. See log for stacktrace.", e);
            System.exit(1);
        }
    }

    private static void loadConfig() {
        String loaderClass = Config.getProperty("config.loader");
        ConfigurationLoader loader = loaderClass == null ? new YamlConfigurationLoader() : Utils
                .<ConfigurationLoader> construct(loaderClass, "configuration loading");
        config = loader.loadConfig();
    }

    private static void init() {
        initBaseDir();
        initPluggableEngines();
        LealoneDatabase.getInstance(); // 提前触发对LealoneDatabase的初始化
        initRouter();
    }

    private static void initBaseDir() {
        if (config.base_dir == null || config.base_dir.isEmpty())
            throw new ConfigurationException("base_dir must be specified and not empty");
        SysProperties.setBaseDir(config.base_dir);

        logger.info("Base dir: {}", config.base_dir);
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
                            StorageEngineManager.getInstance().registerEngine(se);
                        } catch (Exception e) {
                            throw new ConfigurationException("StorageEngine '" + def.name + "' can not found");
                        }
                    }

                    if (Config.getProperty("default.storage.engine") == null)
                        Config.setProperty("default.storage.engine", se.getName());

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
                            TransactionEngineManager.getInstance().registerEngine(te);
                        } catch (Exception e) {
                            throw new ConfigurationException("TransactionEngine '" + def.name + "' can not found");
                        }
                    }

                    if (Config.getProperty("default.transaction.engine") == null)
                        Config.setProperty("default.transaction.engine", te.getName());

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
                            SQLEngineManager.getInstance().registerEngine(se);
                        } catch (Exception e) {
                            throw new ConfigurationException("SQLEngine '" + def.name + "' can not found");
                        }
                    }

                    if (Config.getProperty("default.sql.engine") == null)
                        Config.setProperty("default.sql.engine", se.getName());

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
                            ProtocolServerEngineManager.getInstance().registerEngine(pse);
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
        Router r = RouterHolder.getRouter();
        RouterHolder.setRouter(new TransactionalRouter(r));
    }

    private static void start() throws Exception {
        startProtocolServers();
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
        final String name = server.getName();
        server.init(parameters);
        server.start();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                server.stop();
                logger.info(name + " stopped");
            }
        }, name + "ShutdownHook");
        Runtime.getRuntime().addShutdownHook(t);

        logger.info(name + " started, host: {}, port: {}", server.getHost(), server.getPort());
    }
}
