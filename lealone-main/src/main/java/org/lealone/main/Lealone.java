/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.main;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.CaseInsensitiveMap;
import org.lealone.common.util.ShutdownHookUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Constants;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.PluggableEngine;
import org.lealone.db.PluginManager;
import org.lealone.db.SysProperties;
import org.lealone.main.config.Config;
import org.lealone.main.config.Config.PluggableEngineDef;
import org.lealone.main.config.ConfigLoader;
import org.lealone.main.config.YamlConfigLoader;
import org.lealone.server.ProtocolServer;
import org.lealone.server.ProtocolServerEngine;
import org.lealone.server.TcpServerEngine;
import org.lealone.sql.SQLEngine;
import org.lealone.storage.StorageEngine;
import org.lealone.transaction.TransactionEngine;

public class Lealone {

    private static final Logger logger = LoggerFactory.getLogger(Lealone.class);

    public static void main(String[] args) {
        new Lealone().start(args);
    }

    public static void main(String[] args, Runnable runnable) {
        // 在一个新线程中启动 Lealone
        CountDownLatch latch = new CountDownLatch(1);
        new Thread(() -> {
            new Lealone().start(args, latch);
        }).start();
        try {
            latch.await();
            if (runnable != null)
                runnable.run();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    public static void embed(String[] args) {
        new Lealone().run(true, null);
    }

    public static void runScript(String url, String... sqlScripts) {
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            for (String script : sqlScripts) {
                logger.info("Run script: " + script);
                stmt.executeUpdate("RUNSCRIPT FROM '" + script + "'");
            }
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    private Config config;
    private String baseDir;
    private String host;
    private String port;

    public void start(String[] args) {
        start(args, null);
    }

    public void start(String[] args, CountDownLatch latch) {
        for (int i = 0; args != null && i < args.length; i++) {
            String arg = args[i].trim();
            if (arg.isEmpty())
                continue;
            if (arg.equals("-embed") || arg.equals("-client")) {
                Shell.main(args);
                return;
            } else if (arg.equals("-config")) {
                Config.setProperty("config", args[++i]);
            } else if (arg.equals("-host")) {
                host = args[++i];
            } else if (arg.equals("-port")) {
                port = args[++i];
            } else if (arg.equals("-baseDir")) {
                baseDir = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                continue;
            }
        }
        run(false, latch);
    }

    private void showUsage() {
        println();
        println("Options are case sensitive. Supported options are:");
        println("-------------------------------------------------");
        println("[-help] or [-?]         Print the list of options");
        println("[-baseDir <dir>]        Database base dir");
        println("[-config <file>]        The config file");
        println("[-host <host>]          Tcp server host");
        println("[-port <port>]          Tcp server port");
        println("[-embed]                Embedded mode");
        println("[-client]               Client mode");
        println();
        println("Client or embedded mode options:");
        println("-------------------------------------------------");
        new Shell(null).showClientOrEmbeddedModeOptions();
    }

    private void println() {
        System.out.println();
    }

    private void println(String s) {
        System.out.println(s);
    }

    private void run(boolean embedded, CountDownLatch latch) {
        logger.info("Lealone version: {}", Constants.RELEASE_VERSION);

        try {
            long t = System.currentTimeMillis();

            loadConfig();

            long t1 = (System.currentTimeMillis() - t);
            t = System.currentTimeMillis();

            beforeInit();
            init();
            afterInit(config);

            long t2 = (System.currentTimeMillis() - t);
            t = System.currentTimeMillis();

            if (embedded) {
                if (latch != null)
                    latch.countDown();
                return;
            }

            startProtocolServers();

            long t3 = (System.currentTimeMillis() - t);
            long totalTime = t1 + t2 + t3;
            logger.info("Total time: {} ms (Load config: {} ms, Init: {} ms, Start: {} ms)", totalTime,
                    t1, t2, t3);
            logger.info("Exit with Ctrl+C");

            if (latch != null)
                latch.countDown();

            // 在主线程中运行，避免出现DestroyJavaVM线程
            Thread.currentThread().setName("CheckpointService");
            TransactionEngine te = PluginManager.getPlugin(TransactionEngine.class,
                    Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
            te.getRunnable().run();
        } catch (Exception e) {
            logger.error("Fatal error: unable to start lealone. See log for stacktrace.", e);
            System.exit(1);
        }
    }

    private void loadConfig() {
        ConfigLoader loader;
        String loaderClass = Config.getProperty("config.loader");
        if (loaderClass != null) {
            loader = Utils.construct(loaderClass, "config loading");
        } else {
            loader = new YamlConfigLoader();
        }
        Config config = loader.loadConfig();
        config = Config.mergeDefaultConfig(config);
        if (host != null || port != null) {
            if (host != null)
                config.listen_address = host;
            for (PluggableEngineDef e : config.protocol_server_engines) {
                if (e.enabled && TcpServerEngine.NAME.equalsIgnoreCase(e.name)) {
                    if (host != null)
                        e.parameters.put("host", host);
                    if (port != null)
                        e.parameters.put("port", port);
                    break;
                }
            }
        }
        if (baseDir != null)
            config.base_dir = baseDir;
        loader.applyConfig(config);
        this.config = config;
    }

    protected void beforeInit() {
    }

    protected void afterInit(Config config) {
    }

    private void init() {
        initBaseDir();
        initPluggableEngines();

        long t1 = System.currentTimeMillis();
        LealoneDatabase.getInstance(); // 提前触发对LealoneDatabase的初始化
        long t2 = System.currentTimeMillis();
        logger.info("Init lealone database: " + (t2 - t1) + " ms");
    }

    private void initBaseDir() {
        if (config.base_dir == null || config.base_dir.isEmpty())
            throw new ConfigException("base_dir must be specified and not empty");
        SysProperties.setBaseDir(config.base_dir);

        logger.info("Base dir: {}", new File(config.base_dir).getAbsolutePath());
    }

    // 严格按这样的顺序初始化: storage -> transaction -> sql -> protocol_server
    private void initPluggableEngines() {
        initStorageEngineEngines();
        initTransactionEngineEngines();
        initSQLEngines();
        initProtocolServerEngines();
    }

    private void initStorageEngineEngines() {
        registerAndInitEngines(config.storage_engines, "storage", "default.storage.engine", def -> {
            StorageEngine se = PluginManager.getPlugin(StorageEngine.class, def.name);
            if (se == null) {
                se = Utils.newInstance(def.name);
                PluginManager.register(se);
            }
            return se;
        });
    }

    private void initTransactionEngineEngines() {
        registerAndInitEngines(config.transaction_engines, "transaction", "default.transaction.engine",
                def -> {
                    TransactionEngine te;
                    try {
                        te = PluginManager.getPlugin(TransactionEngine.class, def.name);
                        if (te == null) {
                            te = Utils.newInstance(def.name);
                            PluginManager.register(te);
                        }
                    } catch (Throwable e) {
                        te = PluginManager.getPlugin(TransactionEngine.class,
                                Constants.DEFAULT_TRANSACTION_ENGINE_NAME);
                        if (te == null) {
                            throw e;
                        }
                        logger.warn("Transaction engine " + def.name + " not found, use " + te.getName()
                                + " instead");
                    }
                    return te;
                });
    }

    private void initSQLEngines() {
        registerAndInitEngines(config.sql_engines, "sql", "default.sql.engine", def -> {
            SQLEngine se = PluginManager.getPlugin(SQLEngine.class, def.name);
            if (se == null) {
                se = Utils.newInstance(def.name);
                PluginManager.register(se);
            }
            return se;
        });
    }

    private void initProtocolServerEngines() {
        registerAndInitEngines(config.protocol_server_engines, "protocol server", null, def -> {
            // 如果ProtocolServer的配置参数中没有指定host，那么就取listen_address的值
            if (!def.getParameters().containsKey("host") && config.listen_address != null)
                def.getParameters().put("host", config.listen_address);
            ProtocolServerEngine pse = PluginManager.getPlugin(ProtocolServerEngine.class, def.name);
            if (pse == null) {
                pse = Utils.newInstance(def.name);
                PluginManager.register(pse);
            }
            return pse;
        });
    }

    private static interface CallableTask<V> {
        V call(PluggableEngineDef def) throws Exception;
    }

    private <T> void registerAndInitEngines(List<PluggableEngineDef> engines, String name,
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
                if (def.is_default && defaultEngineKey != null
                        && Config.getProperty(defaultEngineKey) == null)
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
        logger.info("Init " + name + "s" + ": " + (t2 - t1) + " ms");
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

    private void initPluggableEngine(PluggableEngine pe, PluggableEngineDef def) {
        Map<String, String> parameters = def.getParameters();
        if (!parameters.containsKey("base_dir"))
            parameters.put("base_dir", config.base_dir);
        pe.init(parameters);
    }

    private void startProtocolServers() throws Exception {
        if (config.protocol_server_engines != null) {
            for (PluggableEngineDef def : config.protocol_server_engines) {
                if (def.enabled) {
                    ProtocolServerEngine pse = PluginManager.getPlugin(ProtocolServerEngine.class,
                            def.name);
                    ProtocolServer protocolServer = pse.getProtocolServer();
                    startProtocolServer(protocolServer);
                }
            }
        }
    }

    private void startProtocolServer(final ProtocolServer server) throws Exception {
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
