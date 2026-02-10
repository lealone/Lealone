/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.main;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.lealone.common.exceptions.ConfigException;
import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.Logger;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.IOUtils;
import com.lealone.common.util.ShutdownHookUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.Constants;
import com.lealone.db.Database;
import com.lealone.db.LealoneDatabase;
import com.lealone.db.SysProperties;
import com.lealone.db.plugin.PluggableEngine;
import com.lealone.db.plugin.PluginManager;
import com.lealone.db.plugin.PluginObject;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;
import com.lealone.db.session.ServerSession;
import com.lealone.server.ProtocolServer;
import com.lealone.server.ProtocolServerEngine;
import com.lealone.server.TcpServerEngine;
import com.lealone.server.scheduler.GlobalScheduler;
import com.lealone.sql.SQLEngine;
import com.lealone.sql.config.Config;
import com.lealone.sql.config.Config.PluggableEngineDef;
import com.lealone.sql.config.ConfigListener;
import com.lealone.sql.config.CreateConfig;
import com.lealone.storage.StorageEngine;
import com.lealone.transaction.TransactionEngine;

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

    public static void embed() {
        new Lealone().run(true, null);
    }

    public static void executeSql(String url, String sql) {
        try (Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement()) {
            logger.info("Execute sql: " + sql);
            stmt.execute(sql);
        } catch (Exception e) {
            throw DbException.convert(e);
        }
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

    private static void println() {
        System.out.println();
    }

    private static void println(String s) {
        System.out.println(s);
    }

    private static void showUsage() {
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
            } else if (arg.equals("-baseDir")) {
                baseDir = args[++i];
            } else if (arg.equals("-host")) {
                host = args[++i];
            } else if (arg.equals("-port")) {
                port = args[++i];
            } else if (arg.equals("-help") || arg.equals("-?")) {
                showUsage();
                return;
            } else {
                continue;
            }
        }
        run(false, latch);
    }

    private void run(boolean embedded, CountDownLatch latch) {
        logger.info("Lealone version: {}", Constants.RELEASE_VERSION);
        try {
            setGlobalShutdownHook();

            // 1. 加载配置
            long t1 = System.currentTimeMillis();
            loadConfig();
            long loadConfigTime = (System.currentTimeMillis() - t1);

            // 2. 初始化
            long t2 = System.currentTimeMillis();
            SchedulerFactory schedulerFactory = SchedulerFactory.getSchedulerFactory(
                    embedded ? EmbeddedScheduler.class : GlobalScheduler.class,
                    config.scheduler.parameters, false);
            Scheduler scheduler = schedulerFactory.getScheduler();

            beforeInit();
            initBaseDir();
            initPluggableEngines(embedded);

            scheduler.handle(() -> {
                try {
                    // 提前触发对LealoneDatabase的初始化
                    initLealoneDatabase();
                    afterInit(config);
                    long initTime = (System.currentTimeMillis() - t2);
                    // 3. 启动ProtocolServer
                    if (!embedded) {
                        long t3 = System.currentTimeMillis();
                        startProtocolServers();
                        long startTime = (System.currentTimeMillis() - t3);
                        long totalTime = loadConfigTime + initTime + startTime;
                        logger.info("Total time: {} ms (Load config: {} ms, Init: {} ms, Start: {} ms)",
                                totalTime, loadConfigTime, initTime, startTime);
                        logger.info(
                                "Lealone started, jvm pid: {}, "
                                        + "exit with ctrl+c or execute sql command: stop server",
                                ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
                    }
                    // 等所有的Server启动完成后再启动Scheduler
                    // 确保所有的初始PeriodicTask都在单线程中注册
                    schedulerFactory.start();
                    if (latch != null)
                        latch.countDown();
                } catch (Throwable t) {
                    // 在新线程中运行，否则当前调度器无法退出
                    new Thread(() -> exceptionExit(t)).start();
                }
            });
            scheduler.start();
            scheduler.wakeUp(); // 及时唤醒，否则会影响启动速度
            if (!embedded) {
                // 在主线程中运行，避免出现DestroyJavaVM线程
                Thread.currentThread().setName("FsyncService-0");
                TransactionEngine.getDefaultTransactionEngine().getFsyncService().run();
            }
        } catch (Throwable t) {
            exceptionExit(t);
        }
    }

    private void exceptionExit(Throwable t) {
        t = t.getCause() == null ? t : t.getCause();
        logger.error("Fatal error: unable to start lealone. See log for stacktrace.", t);
        System.exit(1);
    }

    private void loadConfig() {
        Config config = createConfig();
        if (baseDir != null)
            config.base_dir = baseDir;
        if (host != null)
            config.listen_address = host;
        config.mergeProtocolServerParameters(TcpServerEngine.NAME, host, port);
        String listenerClass = Config.getProperty("config.listener");
        if (listenerClass != null) {
            ConfigListener listener = Utils.construct(listenerClass, "config listener");
            listener.applyConfig(config);
        }
        this.config = config;
    }

    public static Config createConfig() {
        URL url = null;
        String configUrl = Config.getProperty("config");
        if (configUrl != null) {
            url = Utils.toURL(configUrl);
            logger.warn("Config file not found: " + configUrl);
        }
        if (url == null) {
            url = Utils.toURL("lealone.sql");
            if (url == null)
                url = Utils.toURL("lealone-test.sql");
            if (url == null) {
                logger.info("Use default config");
                return new Config();
            }
        }
        logger.info("Loading config from {}", url);
        try (InputStream is = url.openStream()) {
            String sql = new String(IOUtils.toByteArray(is));
            ServerSession session = new ServerSession(new Database(0, "lealone", null), null, 0);
            CreateConfig createConfig = (CreateConfig) session.parseStatement(sql);
            session.close();
            return createConfig.getConfig();
        } catch (Exception e) {
            throw new ConfigException("Invalid config", e);
        }
    }

    protected void beforeInit() {
    }

    protected void afterInit(Config config) {
    }

    private void initLealoneDatabase() {
        long t1 = System.currentTimeMillis();
        LealoneDatabase.getInstance();
        logger.info("Init lealone database: " + (System.currentTimeMillis() - t1) + " ms");
    }

    private void initBaseDir() {
        if (config.base_dir == null || config.base_dir.isEmpty())
            throw new ConfigException("base_dir must be specified and not empty");
        String baseDir;
        try {
            baseDir = new File(config.base_dir).getCanonicalPath();
        } catch (IOException e) {
            baseDir = new File(config.base_dir).getAbsolutePath();
        }
        SysProperties.setBaseDir(baseDir);
        logger.info("Base dir: {}", baseDir.replace('\\', '/')); // 显示格式跟Loading config一样
    }

    // 严格按这样的顺序初始化: storage -> transaction -> sql -> protocol_server
    private void initPluggableEngines(boolean embedded) {
        registerAndInitEngines(config.storage_engines, StorageEngine.class, "default.storage.engine");
        registerAndInitEngines(config.transaction_engines, TransactionEngine.class,
                "default.transaction.engine");
        registerAndInitEngines(config.sql_engines, SQLEngine.class, "default.sql.engine");
        if (!embedded)
            registerAndInitEngines(config.protocol_server_engines, ProtocolServerEngine.class, null);
    }

    private <PE extends PluggableEngine> void registerAndInitEngines(List<PluggableEngineDef> engines,
            Class<PE> engineClass, String defaultEngineKey) {
        long t1 = System.currentTimeMillis();
        String engineTypeMsg = PluggableEngine.getEngineType(engineClass) + " engine";
        if (engines != null) {
            for (PluggableEngineDef def : engines) {
                if (!def.enabled)
                    continue;
                String name = def.name;
                if (name == null || (name = name.trim()).isEmpty())
                    throw new ConfigException(engineTypeMsg + " name is missing.");

                // 允许后续的访问不用区分大小写
                CaseInsensitiveMap<String> parameters = new CaseInsensitiveMap<>(def.getParameters());
                if (!parameters.containsKey("base_dir"))
                    parameters.put("base_dir", config.base_dir);
                if (engineClass == ProtocolServerEngine.class) {
                    // 如果ProtocolServer的配置参数中没有指定host，那么就取listen_address的值
                    if (!parameters.containsKey("host") && config.listen_address != null)
                        parameters.put("host", config.listen_address);
                }
                def.setParameters(parameters);

                PE pe = PluggableEngine.getEngine(engineClass, name);

                if (def.is_default && defaultEngineKey != null
                        && Config.getProperty(defaultEngineKey) == null)
                    Config.setProperty(defaultEngineKey, name);
                try {
                    pe.init(parameters);
                } catch (Throwable e) {
                    throw new ConfigException("Failed to init " + engineTypeMsg + ": " + name, e);
                }
            }
        }
        logger.info("Init " + engineTypeMsg + "s: " + (System.currentTimeMillis() - t1) + " ms");
    }

    private void startProtocolServers() {
        if (config.protocol_server_engines != null) {
            for (PluggableEngineDef def : config.protocol_server_engines) {
                if (!def.enabled)
                    continue;
                ProtocolServerEngine pse = PluginManager.getPlugin(ProtocolServerEngine.class, def.name);
                ProtocolServer server = pse.getProtocolServer();
                server.setServerEncryptionOptions(config.server_encryption_options);
                String name = server.getName();
                logger.info("Start {}, host: {}, port: {}", name, server.getHost(), server.getPort());
                server.start();
            }
        }
        startPlugins();
    }

    private void startPlugins() {
        List<PluginObject> pluginObjects = LealoneDatabase.getInstance().getAllPluginObjects();
        for (PluginObject pluginObject : pluginObjects) {
            if (pluginObject.isAutoStart())
                pluginObject.start();
        }
    }

    private void setGlobalShutdownHook() {
        ShutdownHookUtils.setGlobalShutdownHook(0, Lealone.class, () -> {
            stop();
        });
    }

    private void stop() {
        for (ProtocolServerEngine pse : PluginManager.getPlugins(ProtocolServerEngine.class)) {
            ProtocolServer server = pse.getProtocolServer();
            if (!server.isStopped()) {
                server.stop();
            }
            pse.close();
        }
        try {
            LealoneDatabase.getInstance().closeAllDatabases(true);
        } catch (Throwable t) {
            // 启动失败时，LealoneDatabase可能没有正常初始化，直接忽略
        }
        // TransactionEngine内部会关闭Scheduler
        for (TransactionEngine te : PluginManager.getPlugins(TransactionEngine.class)) {
            te.close();
        }
        logger.info("Lealone stopped");
    }
}
