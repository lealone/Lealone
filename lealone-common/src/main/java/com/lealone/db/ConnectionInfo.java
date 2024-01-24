/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.StringTokenizer;

import com.lealone.common.exceptions.DbException;
import com.lealone.common.security.SHA256;
import com.lealone.common.util.CaseInsensitiveMap;
import com.lealone.common.util.SortedProperties;
import com.lealone.common.util.StringUtils;
import com.lealone.common.util.Utils;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.session.Session;
import com.lealone.db.session.SessionFactory;
import com.lealone.db.session.SessionSetting;
import com.lealone.storage.fs.impl.encrypt.FilePathEncrypt;

/**
 * Encapsulates the connection settings, including user name and password.
 * 
 * @author H2 Group
 * @author zhh
 */
public class ConnectionInfo implements Cloneable {

    private static final boolean IGNORE_UNKNOWN_SETTINGS = Utils
            .getProperty("lealone.ignore.unknown.settings", false);
    private static final HashSet<String> KNOWN_SETTINGS = new HashSet<>();

    static {
        for (DbSetting setting : DbSetting.values()) {
            add(setting.name());
        }
        for (SessionSetting setting : SessionSetting.values()) {
            add(setting.getName()); // 不用name()，有个特殊的@
        }
        for (ConnectionSetting setting : ConnectionSetting.values()) {
            add(setting.name());
        }
    }

    private static void add(String key) {
        if (SysProperties.CHECK && KNOWN_SETTINGS.contains(key)) {
            DbException.throwInternalError(key);
        }
        KNOWN_SETTINGS.add(key);
    }

    private static boolean isKnownSetting(String s) {
        return KNOWN_SETTINGS.contains(s);
    }

    private final Properties prop = new Properties();
    private String url; // 不包含后面的参数
    private String user;
    private byte[] filePasswordHash;
    private byte[] fileEncryptionKey;
    private byte[] userPasswordHash;
    private byte[] salt;

    /**
     * The database name
     */
    private String dbName;
    private boolean remote;
    private boolean ssl;
    private boolean embedded;
    private String servers;

    private SessionFactory sessionFactory;
    private String sessionFactoryName;

    private Boolean persistent; // 首次调用isPersistent()时才初始化

    private String netFactoryName = Constants.DEFAULT_NET_FACTORY_NAME;
    private int networkTimeout = Constants.DEFAULT_NETWORK_TIMEOUT;
    private boolean traceEnabled;
    private boolean isServiceConnection;

    public ConnectionInfo() {
    }

    /**
     * Create a server connection info object.
     *  
     * @param url
     * @param dbName
     */
    public ConnectionInfo(String url, String dbName) { // 用于server端, 不需要再解析URL了
        this.url = url;
        this.dbName = dbName;

        checkURL();
        url = url.substring(Constants.URL_PREFIX.length());
        // server端接收到的URL不可能是嵌入式的
        if (url.startsWith(Constants.URL_EMBED)) {
            throw DbException.getInternalError("Server backend URL: " + this.url);
        }

        // server端这两参数总是false
        embedded = false;
        remote = false;
    }

    public ConnectionInfo(String url) {
        this(url, (Properties) null);
    }

    /**
     * Create a client connection info object.
     *
     * @param url the database URL
     * @param prop the connection properties
     */
    public ConnectionInfo(String url, Properties prop) { // 用于client端，需要解析URL
        this.url = remapURL(url);

        checkURL();
        readProperties(prop);
        readAndRemoveSettingsFromURL();
        parseURL();

        // 如果url中未指定用户名，默认是root
        String userName = removeProperty(ConnectionSetting.USER, "");
        if (userName.isEmpty() && isEmbedded())
            userName = "ROOT";
        setUserName(userName);

        convertPasswords();
        isServiceConnection = removeProperty(ConnectionSetting.IS_SERVICE_CONNECTION, false);

        netFactoryName = removeProperty(ConnectionSetting.NET_FACTORY_NAME,
                Constants.DEFAULT_NET_FACTORY_NAME);
        networkTimeout = removeProperty(ConnectionSetting.NETWORK_TIMEOUT,
                Constants.DEFAULT_NETWORK_TIMEOUT);

        sessionFactoryName = removeProperty(ConnectionSetting.SESSION_FACTORY_NAME, null);
        initTraceProperty();
        if (isEmbedded()) {
            System.setProperty("lealone.embedded", "true");
        }
    }

    private void checkURL() {
        if (url == null || !url.startsWith(Constants.URL_PREFIX)) {
            throw getFormatException();
        }
    }

    private void readProperties(Properties prop) {
        if (prop == null)
            return;
        for (Entry<Object, Object> e : prop.entrySet()) {
            String key = e.getKey().toString();
            String value = e.getValue().toString();
            addProperty(key, value, false); // 第一次读时，不必检查属性名是否重复
        }
    }

    public void addProperty(String key, String value, boolean checkDuplicate) {
        key = StringUtils.toUpperEnglish(key);
        if (checkDuplicate) {
            String old = prop.getProperty(key);
            if (old != null) {
                if (!old.equals(value)) {
                    throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
                }
                return;
            }
        }

        if (isKnownSetting(key)) {
            prop.put(key, value);
        } else {
            if (IGNORE_UNKNOWN_SETTINGS)
                return;
            boolean ignoreUnknownSetting = getProperty(ConnectionSetting.IGNORE_UNKNOWN_SETTINGS, false);
            if (!ignoreUnknownSetting)
                throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, key);
        }
    }

    // 如果URL中有参数先读出来，然后从URL中移除
    private void readAndRemoveSettingsFromURL() {
        // 支持两种风格的JDBC URL参数语法
        // 1. MySQL的JDBC URL参数语法:
        // .../database[?propertyName1=propertyValue1][&propertyName2=propertyValue2]
        // 数据库名与参数之间用'?'号分隔，不同参数之间用'&'分隔

        // 2.Lealone的JDBC URL参数语法:
        // .../database[;propertyName1=propertyValue1][;propertyName2=propertyValue2]
        // 数据库名与参数之间用';'号分隔，不同参数之间也用';'号分隔
        int idx = url.indexOf('?');
        char splitChar;
        if (idx >= 0) {
            splitChar = '&';
            if (url.indexOf(';') >= 0)
                throw getFormatException(); // 不能同时出现'&'和';'
        } else {
            idx = url.indexOf(';');
            splitChar = ';';
            if (url.indexOf('&') >= 0)
                throw getFormatException(); // 不能出现'&'
        }

        if (idx >= 0) {
            String settings = url.substring(idx + 1);
            url = url.substring(0, idx); // 去掉后面的参数
            String[] list = StringUtils.arraySplit(settings, splitChar);
            for (String setting : list) {
                if (setting.isEmpty()) {
                    continue;
                }
                int equal = setting.indexOf('=');
                if (equal < 0) {
                    throw getFormatException();
                }

                String key = setting.substring(0, equal);
                String value = setting.substring(equal + 1);
                addProperty(key, value, true);// 检查url中设置的参数跟用Properties设置的参数是否重复
            }
        }
    }

    private void parseURL() {
        remote = true;
        dbName = url.substring(Constants.URL_PREFIX.length());

        if (dbName.startsWith(Constants.URL_TCP)) {
            dbName = dbName.substring(Constants.URL_TCP.length());
        } else if (dbName.startsWith(Constants.URL_SSL)) {
            ssl = true;
            dbName = dbName.substring(Constants.URL_SSL.length());
        } else if (dbName.startsWith(Constants.URL_EMBED)) {
            remote = false;
            embedded = true;
            dbName = dbName.substring(Constants.URL_EMBED.length());
            dbName = parseShortName();
        } else {
            throw getFormatException();
        }

        if (remote) {
            if (dbName.startsWith("//")) // 在URL中"//"是可选的
                dbName = dbName.substring("//".length());

            int idx = dbName.indexOf('/');
            if (idx < 0)
                throw getFormatException();
            servers = dbName.substring(0, idx);
            dbName = dbName.substring(idx + 1);
        }
        // 数据库名在内部会对应一个ID，目录名也是用ID表示，所以无需对数据库名的有效性进行复杂的检查
        dbName = dbName.trim();
        if (dbName.isEmpty()) {
            throw DbException.get(ErrorCode.INVALID_DATABASE_NAME_1, "unnamed");
        }
    }

    /**
     * Check if this is a remote connection.
     *
     * @return true if it is
     */
    public boolean isRemote() {
        return remote;
    }

    public void setRemote(boolean b) {
        remote = b;
    }

    /**
     * Check if the referenced database is persistent.
     *
     * @return true if it is
     */
    public boolean isPersistent() {
        if (persistent == null) {
            String v = getProperty("PERSISTENT");
            if (v == null) {
                persistent = embedded;
            } else {
                persistent = parseBoolean(v);
            }
        }
        return persistent.booleanValue();
    }

    public boolean isEmbedded() {
        return embedded;
    }

    public String getServers() {
        return servers;
    }

    private char[] removePassword() {
        Object p = prop.remove(ConnectionSetting.PASSWORD.name());
        if (p == null) {
            return new char[0];
        } else if (p instanceof char[]) {
            return (char[]) p;
        } else {
            return p.toString().toCharArray();
        }
    }

    /**
     * Split the password property into file password and user password if
     * necessary, and convert them to the internal hash format.
     */
    private void convertPasswords() {
        char[] password = removePassword();
        boolean passwordHash = removeProperty(ConnectionSetting.PASSWORD_HASH, false);
        if (getProperty(DbSetting.CIPHER.getName(), null) != null) {
            // split password into (filePassword+' '+userPassword)
            int space = -1;
            for (int i = 0, len = password.length; i < len; i++) {
                if (password[i] == ' ') {
                    space = i;
                    break;
                }
            }
            if (space < 0) {
                throw DbException.get(ErrorCode.WRONG_PASSWORD_FORMAT);
            }
            char[] np = new char[password.length - space - 1];
            char[] filePassword = new char[space];
            System.arraycopy(password, space + 1, np, 0, np.length);
            System.arraycopy(password, 0, filePassword, 0, space);
            Arrays.fill(password, (char) 0);
            password = np;
            fileEncryptionKey = FilePathEncrypt.getPasswordBytes(filePassword);
            filePasswordHash = hashPassword(passwordHash, "file", filePassword);
        }
        userPasswordHash = hashPassword(passwordHash, user, password);
    }

    private static byte[] hashPassword(boolean passwordHash, String userName, char[] passwordChars) {
        if (passwordHash) {
            return StringUtils.convertHexToBytes(new String(passwordChars));
        }
        return createUserPasswordHash(userName, passwordChars);
    }

    public static byte[] createUserPasswordHash(String userName, char[] passwordChars) {
        // 不能用用户名和密码组成hash，否则重命名用户后将不能通过原来的密码登录
        // TODO 如果不用固定的名称是否还有更好办法？
        userName = Constants.PROJECT_NAME;
        if (userName.length() == 0 && passwordChars.length == 0) {
            return new byte[0];
        }
        return SHA256.getKeyPasswordHash(userName, passwordChars);
    }

    public String getDatabaseName() {
        return dbName;
    }

    private String parseShortName() {
        String n = dbName;
        if (n.endsWith(":")) {
            n = null;
        }
        if (n != null) {
            StringTokenizer tokenizer = new StringTokenizer(n, "/\\:,;");
            while (tokenizer.hasMoreTokens()) {
                n = tokenizer.nextToken();
            }
        }
        if (n == null || n.length() == 0) {
            n = "unnamed";
        }
        return n;
    }

    /**
     * Get the name of the user.
     *
     * @return the user name
     */
    public String getUserName() {
        return user;
    }

    /**
     * Get the user password hash.
     *
     * @return the password hash
     */
    public byte[] getUserPasswordHash() {
        return userPasswordHash;
    }

    public byte[] getSalt() {
        return salt;
    }

    public void setSalt(byte[] salt) {
        this.salt = salt;
    }

    /**
     * Get the file password hash if it is set.
     *
     * @return the password hash or null
     */
    public byte[] getFilePasswordHash() {
        return filePasswordHash;
    }

    public byte[] getFileEncryptionKey() {
        return fileEncryptionKey;
    }

    /**
     * Get the property keys.
     *
     * @return the property keys
     */
    public String[] getKeys() {
        return getKeys(false);
    }

    public String[] getKeys(boolean ignoreClientSettings) {
        Properties prop = this.prop;
        if (ignoreClientSettings) {
            prop = new Properties();
            prop.putAll(this.prop);
            // 这些参数不需要传给server
            prop.remove(ConnectionSetting.IS_SHARED.name());
            prop.remove(ConnectionSetting.MAX_SHARED_SIZE.name());
            prop.remove(ConnectionSetting.NET_CLIENT_COUNT.name());
        }
        String[] keys = new String[prop.size()];
        prop.keySet().toArray(keys);
        return keys;
    }

    /**
     * Check if this is a remote connection with SSL enabled.
     *
     * @return true if it is
     */
    public boolean isSSL() {
        return ssl;
    }

    /**
     * Overwrite the user name. The user name is case-insensitive and stored in
     * uppercase. English conversion is used.
     *
     * @param name the user name
     */
    public void setUserName(String name) {
        this.user = StringUtils.toUpperEnglish(name);
    }

    /**
     * Set the user password hash.
     *
     * @param hash the new hash value
     */
    public void setUserPasswordHash(byte[] hash) {
        this.userPasswordHash = hash;
    }

    /**
     * Set the file password hash.
     *
     * @param hash the new hash value
     */
    public void setFilePasswordHash(byte[] hash) {
        this.filePasswordHash = hash;
    }

    public void setFileEncryptionKey(byte[] key) {
        this.fileEncryptionKey = key;
    }

    public void setProperty(ConnectionSetting key, String value) {
        setProperty(key.name(), value);
    }

    /**
     * Overwrite a property.
     *
     * @param key the property name
     * @param value the value
     */
    public void setProperty(String key, String value) {
        // value is null if the value is an object
        if (value != null) {
            prop.setProperty(key, value);
        }
    }

    public String getProperty(ConnectionSetting key) {
        Object value = prop.get(key.name());
        if (value == null || !(value instanceof String)) {
            return null;
        }
        return value.toString();
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @return the value as a String
     */
    public String getProperty(String key) {
        Object value = prop.get(key);
        if (value == null || !(value instanceof String)) {
            return null;
        }
        return value.toString();
    }

    public int getProperty(ConnectionSetting key, int defaultValue) {
        return getProperty(key.name(), defaultValue);
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    public int getProperty(String key, int defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            DbException.throwInternalError(key);
        }
        String s = getProperty(key);
        return s == null ? defaultValue : Integer.parseInt(s);
    }

    public String getProperty(ConnectionSetting key, String defaultValue) {
        return getProperty(key.name(), defaultValue);
    }

    /**
     * Get the value of the given property.
     *
     * @param key the property key
     * @param defaultValue the default value
     * @return the value as a String
     */
    public String getProperty(String key, String defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            DbException.throwInternalError(key);
        }
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    public boolean getProperty(ConnectionSetting key, boolean defaultValue) {
        return getProperty(key.name(), defaultValue);
    }

    /**
     * Get a boolean property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean getProperty(String key, boolean defaultValue) {
        String x = getProperty(key, null);
        if (x == null) {
            return defaultValue;
        }
        return parseBoolean(x);
    }

    private boolean parseBoolean(String x) {
        // support 0 / 1 (like the parser)
        if (x.length() == 1 && Character.isDigit(x.charAt(0))) {
            return Integer.parseInt(x) != 0;
        }
        return Boolean.parseBoolean(x);
    }

    /**
     * Remove a boolean property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    public boolean removeProperty(String key, boolean defaultValue) {
        String x = removeProperty(key, null);
        return x == null ? defaultValue : parseBoolean(x);
    }

    public boolean removeProperty(ConnectionSetting key, boolean defaultValue) {
        return removeProperty(key.name(), defaultValue);
    }

    public String removeProperty(ConnectionSetting key, String defaultValue) {
        return removeProperty(key.name(), defaultValue);
    }

    public int removeProperty(ConnectionSetting key, int defaultValue) {
        String x = removeProperty(key, null);
        return x == null ? defaultValue : Integer.parseInt(x);
    }

    /**
     * Remove a String property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    public String removeProperty(String key, String defaultValue) {
        if (SysProperties.CHECK && !isKnownSetting(key)) {
            DbException.throwInternalError(key);
        }
        Object x = prop.remove(key);
        return x == null ? defaultValue : x.toString();
    }

    /**
     * Get the database URL.
     *
     * @return the URL
     */
    public String getURL() {
        return url;
    }

    /**
     * Generate an URL format exception.
     *
     * @return the exception
     */
    private DbException getFormatException() {
        return DbException.get(ErrorCode.URL_FORMAT_ERROR_2, Constants.URL_FORMAT, url);
    }

    public Session createSession() {
        return getSessionFactory().createSession(this).get();
    }

    public SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            if (sessionFactoryName != null) {
                sessionFactory = PluginManager.getPlugin(SessionFactory.class, sessionFactoryName);
                if (sessionFactory == null)
                    throw DbException.get(ErrorCode.PLUGIN_NOT_FOUND_1, sessionFactoryName);
            } else {
                // 要使用反射，避免编译期依赖
                try {
                    String className;
                    if (remote) {
                        className = "com.lealone.client.session.ClientSessionFactory";
                    } else {
                        className = "com.lealone.db.session.ServerSessionFactory";
                    }
                    sessionFactory = (SessionFactory) Class.forName(className).getMethod("getInstance")
                            .invoke(null);
                } catch (Exception e) {
                    throw DbException.convert(e);
                }
            }
        }
        return sessionFactory;
    }

    public void setSessionFactory(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    private static String remapURL(String url) {
        String urlMap = SysProperties.URL_MAP;
        if (urlMap != null && urlMap.length() > 0) {
            try {
                SortedProperties prop;
                prop = SortedProperties.loadProperties(urlMap);
                String url2 = prop.getProperty(url);
                if (url2 == null) {
                    prop.put(url, "");
                    prop.store(urlMap);
                } else {
                    url2 = url2.trim();
                    if (url2.length() > 0) {
                        return url2;
                    }
                }
            } catch (IOException e) {
                throw DbException.convert(e);
            }
        }
        return url;
    }

    public Properties getProperties() {
        return prop;
    }

    public CaseInsensitiveMap<String> getConfig() {
        CaseInsensitiveMap<String> config = new CaseInsensitiveMap<>(getProperties());
        if (getNetFactoryName() != null)
            config.put(ConnectionSetting.NET_FACTORY_NAME.name(), getNetFactoryName());
        if (getNetworkTimeout() > 0)
            config.put(ConnectionSetting.NETWORK_TIMEOUT.name(), String.valueOf(getNetworkTimeout()));
        return config;
    }

    public ConnectionInfo copy(String newServer) {
        ConnectionInfo ci = new ConnectionInfo();
        ci.prop.putAll(prop);
        StringBuilder buff = new StringBuilder(Constants.URL_PREFIX);
        buff.append(Constants.URL_TCP).append("//").append(newServer).append('/').append(dbName);
        ci.url = buff.toString();
        ci.user = user;
        ci.filePasswordHash = filePasswordHash;
        ci.fileEncryptionKey = fileEncryptionKey;
        ci.userPasswordHash = userPasswordHash;
        ci.dbName = dbName;
        ci.remote = remote;
        ci.ssl = ssl;
        ci.embedded = embedded;
        ci.servers = newServer;
        ci.sessionFactory = sessionFactory;
        ci.sessionFactoryName = sessionFactoryName;
        ci.persistent = persistent;
        ci.netFactoryName = netFactoryName;
        ci.networkTimeout = networkTimeout;
        ci.traceEnabled = traceEnabled;
        ci.singleThreadCallback = singleThreadCallback;
        ci.scheduler = scheduler;
        return ci;
    }

    public String getNetFactoryName() {
        return netFactoryName;
    }

    public void setNetworkTimeout(int milliseconds) {
        networkTimeout = milliseconds;
    }

    public int getNetworkTimeout() {
        return networkTimeout;
    }

    public boolean isTraceEnabled() {
        return traceEnabled;
    }

    public boolean isTraceDisabled() {
        return !traceEnabled;
    }

    public void initTraceProperty() {
        String v = getProperty(ConnectionSetting.TRACE_ENABLED);
        if (v != null) {
            traceEnabled = Boolean.parseBoolean(v);
            if (!traceEnabled)
                return;
        }
        traceEnabled = getProperty(DbSetting.TRACE_LEVEL_FILE.getName()) != null
                || getProperty(DbSetting.TRACE_LEVEL_SYSTEM_OUT.getName()) != null;
    }

    public boolean isServiceConnection() {
        return isServiceConnection;
    }

    private boolean singleThreadCallback;

    public boolean isSingleThreadCallback() {
        return singleThreadCallback;
    }

    public void setSingleThreadCallback(boolean singleThreadCallback) {
        this.singleThreadCallback = singleThreadCallback;
    }

    private int databaseId = -1;

    public int getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(int databaseId) {
        this.databaseId = databaseId;
    }

    private Scheduler scheduler;

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }
}
