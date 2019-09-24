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

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Constants;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluggableEngine;
import org.lealone.db.SysProperties;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.Config.PluggableEngineDef;
import org.lealone.p2p.config.ConfigDescriptor;
import org.lealone.p2p.config.ConfigLoader;
import org.lealone.p2p.config.YamlConfigLoader;
import org.lealone.p2p.server.ClusterMetaData;
import org.lealone.p2p.server.P2pServerEngine;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.server.ProtocolServerEngineManager;
import org.lealone.sql.SQLEngine;
import org.lealone.sql.SQLEngineManager;
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

            loadConfig(args);

            long t1 = (System.currentTimeMillis() - t);
            t = System.currentTimeMillis();

            init();

            long t2 = (System.currentTimeMillis() - t);
            t = System.currentTimeMillis();

            ProtocolServer mainProtocolServer = start();

            long t3 = (System.currentTimeMillis() - t);
            logger.info("Total time: {} ms (Load config: {} ms, Init: {} ms, Start: {} ms)", (t1 + t2 + t3), t1, t2,
                    t3);

            // 在主线程中运行，避免出现DestroyJavaVM线程
            if (mainProtocolServer != null)
                mainProtocolServer.getRunnable().run();
        } catch (Exception e) {
            logger.error("Fatal error: unable to start lealone. See log for stacktrace.", e);
            System.exit(1);
        }
    }

    private static void loadConfig(String[] args) {
        ConfigLoader loader;
        String loaderClass = Config.getProperty("config.loader");
        if (loaderClass != null && Lealone.class.getResource("/" + loaderClass.replace('.', '/') + ".class") != null) {
            loader = Utils.<ConfigLoader> construct(loaderClass, "configuration loading");
        } else {
            loader = new YamlConfigLoader();
        }
        if (args != null && args.length >= 2 && args[0].equalsIgnoreCase("-cluster")) {
            String nodeId = args[1];
            config = loader.loadConfig(true);
            config.base_dir = config.base_dir + "/node" + nodeId;
            config.listen_address = "127.0.0." + nodeId;
            for (PluggableEngineDef e : config.protocol_server_engines) {
                if (P2pServerEngine.NAME.equalsIgnoreCase(e.name)) {
                    e.enabled = true;
                }
            }
            ConfigDescriptor.applyConfig(config);
        } else {
            config = loader.loadConfig();
        }
    }

    private static void init() {
        initBaseDir();
        initPluggableEngines();

        long t1 = System.currentTimeMillis();
        LealoneDatabase.getInstance(); // 提前触发对LealoneDatabase的初始化
        long t2 = System.currentTimeMillis();
        logger.info("Init lealone database: " + (t2 - t1) + "ms");

        // 如果启用了集群，集群的元数据表通过嵌入式的方式访问
        if (config.protocol_server_engines != null) {
            for (PluggableEngineDef def : config.protocol_server_engines) {
                if (def.enabled && P2pServerEngine.NAME.equalsIgnoreCase(def.name)) {
                    ClusterMetaData.init(LealoneDatabase.getInstance().getInternalConnection());
                    break;
                }
            }
        }
    }

    private static void initBaseDir() {
        if (config.base_dir == null || config.base_dir.isEmpty())
            throw new ConfigException("base_dir must be specified and not empty");
        SysProperties.setBaseDir(config.base_dir);

        logger.info("Base dir: {}", config.base_dir);
    }

    // 严格按这样的顺序初始化: storage -> transaction -> sql -> protocol_server
    private static void initPluggableEngines() {
        initStorageEngineEngines();
        initTransactionEngineEngines();
        initSQLEngines();
        initProtocolServerEngines();
    }

    private static void initStorageEngineEngines() {
        registerAndInitEngines(config.storage_engines, "storage", "default.storage.engine", def -> {
            StorageEngine se = StorageEngineManager.getInstance().getEngine(def.name);
            if (se == null) {
                Class<?> clz = Utils.loadUserClass(def.name);
                se = (StorageEngine) clz.newInstance();
                StorageEngineManager.getInstance().registerEngine(se);
            }
            return se;
        });
    }

    private static void initTransactionEngineEngines() {
        registerAndInitEngines(config.transaction_engines, "transaction", "default.transaction.engine", def -> {
            TransactionEngine te;
            try {
                te = TransactionEngineManager.getInstance().getEngine(def.name);
                if (te == null) {
                    Class<?> clz = Utils.loadUserClass(def.name);
                    te = (TransactionEngine) clz.newInstance();
                    TransactionEngineManager.getInstance().registerEngine(te);
                }
            } catch (Throwable e) {
                te = TransactionEngineManager.getInstance().getEngine(Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
                if (te == null) {
                    throw e;
                }
                logger.warn("Transaction engine " + def.name + " not found, use " + te.getName() + " instead");
            }
            return te;
        });
    }

    private static void initSQLEngines() {
        registerAndInitEngines(config.sql_engines, "sql", "default.sql.engine", def -> {
            SQLEngine se = SQLEngineManager.getInstance().getEngine(def.name);
            if (se == null) {
                Class<?> clz = Utils.loadUserClass(def.name);
                se = (SQLEngine) clz.newInstance();
                SQLEngineManager.getInstance().registerEngine(se);
            }
            return se;
        });
    }

    private static void initProtocolServerEngines() {
        registerAndInitEngines(config.protocol_server_engines, "protocol server", null, def -> {
            // 如果ProtocolServer的配置参数中没有指定host，那么就取listen_address的值
            if (!def.getParameters().containsKey("host") && config.listen_address != null)
                def.getParameters().put("host", config.listen_address);
            ProtocolServerEngine pse = ProtocolServerEngineManager.getInstance().getEngine(def.name);
            if (pse == null) {
                Class<?> clz = Utils.loadUserClass(def.name);
                pse = (ProtocolServerEngine) clz.newInstance();
                ProtocolServerEngineManager.getInstance().registerEngine(pse);
            }
            return pse;
        });
    }

    private static interface CallableTask<V> {
        V call(PluggableEngineDef def) throws Exception;
    }

    private static <T> void registerAndInitEngines(List<PluggableEngineDef> engines, String name,
            String defaultEngineKey, CallableTask<T> callableTask) {
        long t1 = System.currentTimeMillis();
        if (engines != null) {
            name += " engine";
            for (PluggableEngineDef def : engines) {
                if (!def.enabled)
                    continue;

                // 允许后续的访问不用区分大小写
                CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(def.getParameters());
                def.setParameters(parameters);

                checkName(name, def);
                T result;
                try {
                    result = callableTask.call(def);
                } catch (Throwable e) {
                    String msg = "Failed to register " + name + ": " + def.name;
                    checkException(msg, e);
                    return;
                }
                PluggableEngine pe = (PluggableEngine) result;
                if (defaultEngineKey != null && Config.getProperty(defaultEngineKey) == null)
                    Config.setProperty(defaultEngineKey, pe.getName());
                try {
                    initPluggableEngine(pe, def);
                } catch (Throwable e) {
                    String msg = "Failed to init " + name + ": " + def.name;
                    checkException(msg, e);
                }
            }
        }
        long t2 = System.currentTimeMillis();
        logger.info("Init " + name + "s" + ": " + (t2 - t1) + "ms");
    }

    private static void checkException(String msg, Throwable e) {
        if (e instanceof ConfigException)
            throw (ConfigException) e;
        else if (e instanceof RuntimeException)
            throw new ConfigException(msg, e);
        else
            logger.warn(msg, e);
    }

    private static void checkName(String engineName, PluggableEngineDef def) {
        if (def.name == null || def.name.trim().isEmpty())
            throw new ConfigException(engineName + " name is missing.");
    }

    private static void initPluggableEngine(PluggableEngine pe, PluggableEngineDef def) {
        Map<String, String> parameters = def.getParameters();
        if (!parameters.containsKey("base_dir"))
            parameters.put("base_dir", config.base_dir);
        pe.init(parameters);
    }

    private static ProtocolServer start() throws Exception {
        return startProtocolServers();
    }

    private static ProtocolServer startProtocolServers() throws Exception {
        ProtocolServer mainProtocolServer = null;
        if (config.protocol_server_engines != null) {
            for (PluggableEngineDef def : config.protocol_server_engines) {
                if (def.enabled) {
                    ProtocolServerEngine pse = ProtocolServerEngineManager.getInstance().getEngine(def.name);
                    ProtocolServer protocolServer = pse.getProtocolServer();
                    if (protocolServer.runInMainThread())
                        mainProtocolServer = protocolServer;
                    startProtocolServer(protocolServer);
                }
            }
        }
        return mainProtocolServer;
    }

    private static void startProtocolServer(final ProtocolServer server) throws Exception {
        server.setServerEncryptionOptions(config.server_encryption_options);
        server.start();
        final String name = server.getName();
        ShutdownHookUtils.addShutdownHook(server, () -> {
            server.stop();
            logger.info(name + " stopped");
        });
        logger.info(name + " started, host: {}, port: {}", server.getHost(), server.getPort());
    }
}
