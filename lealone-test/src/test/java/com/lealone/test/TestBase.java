/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.logging.LoggerFactory;
import com.lealone.common.logging.impl.ConsoleLoggerFactory;
import com.lealone.common.trace.TraceSystem;
import com.lealone.db.ConnectionSetting;
import com.lealone.db.Constants;
import com.lealone.db.DbSetting;
import com.lealone.db.SysProperties;
import com.lealone.main.config.Config;
import com.lealone.storage.fs.FileUtils;

public class TestBase extends Assert {

    public static interface SqlExecutor {
        void execute(String sql);
    }

    // 一个标记接口，在Embedded模式下运行的测试类
    public static interface EmbeddedTest {
    }

    // 一个标记接口，在Client-Server模式下运行的测试类
    public static interface ClientServerTest {
    }

    // 一个标记接口，指示那些通过main方式运行的测试类
    public static interface MainTest {
    }

    // 一个标记接口，指示那些还有bug的测试类
    public static interface TodoTest {
    }

    public static String url;
    public static final String DEFAULT_STORAGE_ENGINE_NAME = getDefaultStorageEngineName();
    public static final String TEST_BASE_DIR = "." + File.separatorChar + "target" + File.separatorChar
            + "test-data";
    public static final String TEST_DIR = TEST_BASE_DIR + File.separatorChar + "test";
    public static final String TEST = "test";
    public static final String LEALONE = "lealone";
    public static final String DEFAULT_DB_NAME = TEST;
    public static final String DEFAULT_USER = "root";
    public static final String DEFAULT_PASSWORD = "";
    public static final int NETWORK_TIMEOUT_MILLISECONDS = Integer.MAX_VALUE; // 方便在eclipse中调试代码

    static {
        System.setProperty("java.io.tmpdir", TEST_DIR + File.separatorChar + "tmp");
        System.setProperty("lealone.lob.client.max.size.memory", "2048");

        Config.setProperty("client.trace.directory", joinDirs("client_trace"));
        SysProperties.setBaseDir(TEST_DIR);

        if (Config.getProperty("default.storage.engine") == null)
            Config.setProperty("default.storage.engine", getDefaultStorageEngineName());

        setConsoleLoggerFactory();
    }

    public TestBase() {
    }

    // 这个在eclipse的console下输出结果并不快，反而会拖慢启动速度，在cmd窗口下才有效果
    // 但是首次执行LoggerFactory.getLogger(String)很快
    public static void setConsoleLoggerFactory() {
        System.setProperty(LoggerFactory.LOGGER_FACTORY_CLASS_NAME,
                ConsoleLoggerFactory.class.getName());
    }

    public static String getDefaultStorageEngineName() {
        return "AOSE";
    }

    protected String dbName = DEFAULT_DB_NAME;
    protected String user = DEFAULT_USER;
    protected String password = DEFAULT_PASSWORD;

    private final Map<String, String> connectionParameters = new HashMap<>();
    // private String storageEngineName = getDefaultStorageEngineName();
    private boolean embedded = false;
    private boolean inMemory = false;
    private boolean mysqlUrlStyle = false;
    private boolean ssl = false;

    private String host = Constants.DEFAULT_HOST;
    private int port = Constants.DEFAULT_TCP_PORT;

    private String netFactoryName = Constants.DEFAULT_NET_FACTORY_NAME;

    public static String joinDirs(String... dirs) {
        StringBuilder s = new StringBuilder(TEST_DIR);
        for (String dir : dirs)
            s.append(File.separatorChar).append(dir);
        return s.toString();
    }

    public synchronized TestBase addConnectionParameter(DbSetting key, String value) {
        connectionParameters.put(key.name(), value);
        return this;
    }

    public synchronized TestBase addConnectionParameter(ConnectionSetting key, String value) {
        connectionParameters.put(key.name(), value);
        return this;
    }

    public synchronized TestBase addConnectionParameter(String key, String value) {
        connectionParameters.put(key, value);
        return this;
    }

    public synchronized TestBase addConnectionParameter(String key, Object value) {
        connectionParameters.put(key, value.toString());
        return this;
    }

    public synchronized TestBase addConnectionParameter(ConnectionSetting key, Object value) {
        connectionParameters.put(key.name(), value.toString());
        return this;
    }

    public TestBase enableTrace() {
        return enableTrace(TraceSystem.INFO);
    }

    public TestBase enableTrace(int level) {
        addConnectionParameter("TRACE_LEVEL_SYSTEM_OUT", level + "");
        addConnectionParameter("TRACE_LEVEL_FILE", level + "");
        return this;
    }

    public TestBase enableSSL() {
        ssl = true;
        return this;
    }

    public TestBase setStorageEngineName(String name) {
        // storageEngineName = name;
        return this;
    }

    public TestBase setNetFactoryName(String name) {
        netFactoryName = name;
        return this;
    }

    public TestBase setEmbedded(boolean embedded) {
        this.embedded = embedded;
        return this;
    }

    public boolean isEmbedded() {
        return embedded;
    }

    public TestBase setInMemory(boolean inMemory) {
        this.inMemory = inMemory;
        return this;
    }

    public TestBase setMysqlUrlStyle(boolean mysqlUrlStyle) {
        this.mysqlUrlStyle = mysqlUrlStyle;
        return this;
    }

    public String getHost() {
        return host;
    }

    public TestBase setHost(String host) {
        this.host = host;
        return this;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public TestBase setPort(int port) {
        this.port = port;
        return this;
    }

    public String getHostAndPort() {
        return host + ":" + port;
    }

    public void printURL() {
        System.out.println("JDBC URL: " + getURL());
        System.out.println();
    }

    public synchronized String getURL() {
        return getURL(dbName);
    }

    public synchronized String getURL(String user, String password) {
        connectionParameters.put("user", user);
        connectionParameters.put("password", password);
        return getURL();
    }

    public synchronized String getURL(String dbName) {
        if (url != null)
            return url;
        // addConnectionParameter(DbSetting.DATABASE_TO_UPPER, "false");
        // addConnectionParameter(DbSetting.ALIAS_COLUMN_NAME, "true");
        // addConnectionParameter(ConnectionSetting.IGNORE_UNKNOWN_SETTINGS, "true");
        // addConnectionParameter(DbSetting.DEFAULT_STORAGE_ENGINE, storageEngineName);

        if (!connectionParameters.containsKey("user")) {
            addConnectionParameter("user", user);
            addConnectionParameter("password", password);
        }
        addConnectionParameter(ConnectionSetting.NETWORK_TIMEOUT,
                String.valueOf(NETWORK_TIMEOUT_MILLISECONDS));

        StringBuilder url = new StringBuilder(100);

        url.append(Constants.URL_PREFIX);
        if (inMemory) {
            addConnectionParameter(DbSetting.PERSISTENT, "false");
        }

        if (embedded) {
            url.append(Constants.URL_EMBED);
            if (!inMemory)
                url.append(TEST_DIR).append('/');
        } else {
            if (ssl)
                url.append(Constants.URL_SSL);
            else
                url.append(Constants.URL_TCP);
            url.append("//").append(host).append(':').append(port).append('/');
        }

        char firstSeparatorChar = ';';
        char separatorChar = ';';
        if (mysqlUrlStyle) {
            firstSeparatorChar = '?';
            separatorChar = '&';
        }

        url.append(dbName).append(firstSeparatorChar).append(ConnectionSetting.NET_FACTORY_NAME.name())
                .append("=").append(netFactoryName);

        for (Map.Entry<String, String> e : connectionParameters.entrySet())
            url.append(separatorChar).append(e.getKey()).append('=').append(e.getValue());

        return url.toString();
    }

    public Connection getConnection() throws Exception {
        return DriverManager.getConnection(getURL());
    }

    public Connection getConnection(String dbName) throws Exception {
        return DriverManager.getConnection(getURL(dbName));
    }

    public Connection getConnection(String user, String password) throws Exception {
        return DriverManager.getConnection(getURL(user, password));
    }

    public void assertException(Exception e, int expectedErrorCode) {
        assertTrue(e instanceof DbException);
        assertEquals(expectedErrorCode, ((DbException) e).getErrorCode());
        // p(e.getMessage());
    }

    public static void assertErrorCode(Exception e, int errorCode) {
        Throwable cause = e instanceof SQLException ? e : e.getCause();
        assertTrue(cause instanceof SQLException);
        assertEquals(errorCode, ((SQLException) cause).getErrorCode());
    }

    public static void p(Object o) {
        System.out.println(o);
    }

    public static void p() {
        System.out.println();
    }

    public static void deleteFileRecursive(String path) {
        // 避免误删除
        if (!path.startsWith(TEST_BASE_DIR)) {
            throw new RuntimeException(
                    "invalid path: " + path + ", must be start with: " + TEST_BASE_DIR);
        }
        FileUtils.deleteRecursive(path, false);
    }

    public static int printResultSet(ResultSet rs) {
        return printResultSet(rs, true);
    }

    public static int printResultSet(ResultSet rs, boolean closeResultSet) {
        int count = 0;
        try {
            int n = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= n; i++) {
                    System.out.print(rs.getString(i) + " ");
                }
                count++;
                System.out.println();
            }
            if (closeResultSet)
                rs.close();
            System.out.println();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return count;
    }
}
