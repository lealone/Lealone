/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.engine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.lealone.api.ErrorCode;
import org.lealone.message.DbException;
import org.lealone.security.SHA256;
import org.lealone.util.New;
import org.lealone.util.SortedProperties;
import org.lealone.util.StringUtils;
import org.lealone.util.Utils;

/**
 * Encapsulates the connection settings, including user name and password.
 */
public class ConnectionInfo implements Cloneable {
    private static final HashSet<String> KNOWN_SETTINGS = New.hashSet();

    static {
        ArrayList<String> list = SetTypes.getTypes();
        HashSet<String> set = KNOWN_SETTINGS;
        set.addAll(list);
        String[] connectionSettings = { "ACCESS_MODE_DATA", "AUTOCOMMIT", "CIPHER", "CREATE", "CACHE_TYPE", "FILE_LOCK",
                "IGNORE_UNKNOWN_SETTINGS", "IFEXISTS", "INIT", "PASSWORD", "RECOVER", "RECOVER_TEST", "USER", "AUTO_SERVER",
                "AUTO_SERVER_PORT", "NO_UPGRADE", "AUTO_RECONNECT", "OPEN_NEW", "PAGE_SIZE", "PASSWORD_HASH", "JMX",
                "ZOOKEEPER_SESSION_TIMEOUT", "USE_H2_CLUSTER_MODE" };
        for (String key : connectionSettings) {
            if (SysProperties.CHECK && set.contains(key)) {
                DbException.throwInternalError(key);
            }
            set.add(key);
        }
    }

    private static boolean isKnownSetting(String s) {
        return KNOWN_SETTINGS.contains(s);
    }

    private Properties prop = new Properties();
    private String url; //不包含后面的参数
    private String user;
    private byte[] filePasswordHash;
    private byte[] userPasswordHash;

    /**
     * The database name
     */
    private String dbName;
    private boolean remote;
    private boolean ssl;
    private boolean dynamic;

    private SessionFactory sessionFactory;
    private SessionInterface session;

    /**
     * Create a server connection info object.
     *  
     * @param url
     * @param dbName
     */
    public ConnectionInfo(String url, String dbName) { //用于server端, 不需要再解析url了
        this.url = url;
        this.dbName = dbName;
    }

    /**
     * Create a client connection info object.
     *
     * @param url the database URL
     * @param info the connection properties
     */
    public ConnectionInfo(String url, Properties info) { //用于client端，需要解析url
        this.url = url;
        url = remapURL(url);
        if (!url.startsWith(Constants.URL_PREFIX)) {
            throw getFormatException();
        }
        readProperties(info);
        readSettingsFromURL();
        setUserName(removeProperty("USER", ""));
        convertPasswords();
        //如果url带参数，在readSettingsFromURL()中会改变this.url的值
        //所以这里用this.url
        this.dbName = this.url.substring(Constants.URL_PREFIX.length());
        parseName();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        ConnectionInfo clone = (ConnectionInfo) super.clone();
        clone.prop = (Properties) prop.clone();
        clone.filePasswordHash = Utils.cloneByteArray(filePasswordHash);
        clone.userPasswordHash = Utils.cloneByteArray(userPasswordHash);
        return clone;
    }

    private void parseName() {
        remote = true;
        if (dbName.startsWith(Constants.URL_TCP)) {
            dbName = dbName.substring(Constants.URL_TCP.length());
        } else if (dbName.startsWith(Constants.URL_SSL)) {
            ssl = true;
            dbName = dbName.substring(Constants.URL_SSL.length());
        } else if (dbName.startsWith(Constants.URL_EMBED)) {
            remote = false;
            dbName = dbName.substring(Constants.URL_EMBED.length());
        } else if (dbName.startsWith(Constants.URL_DYNAMIC)) {
            dynamic = true;
            dbName = dbName.substring(Constants.URL_DYNAMIC.length());
        } else {
            throw getFormatException();
        }
    }

    public void setBaseDir(String dir) {
        //TODO 看看是否可以删 除
    }

    /**
     * Check if this is a remote connection.
     *
     * @return true if it is
     */
    public boolean isRemote() {
        return remote;
    }

    public void readProperties(Properties info) {
        Object[] list = new Object[info.size()];
        info.keySet().toArray(list);
        DbSettings s = null;
        for (Object k : list) {
            String key = StringUtils.toUpperEnglish(k.toString());
            if (prop.containsKey(key)) {
                throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
            }
            Object value = info.get(k);
            if (isKnownSetting(key)) {
                prop.put(key, value);
            } else {
                if (s == null) {
                    s = getDbSettings();
                }
                if (s.containsKey(key)) {
                    prop.put(key, value);
                }
            }
        }
    }

    private void readSettingsFromURL() {
        DbSettings dbSettings = DbSettings.getInstance();
        //Lealone的JDBC URL语法:
        //jdbc:lealone:tcp://[host:port],[host:port].../[database][;propertyName1][=propertyValue1][;propertyName2][=propertyValue2]
        //数据库名与参数之间用';'号分隔，不同参数之间也用';'号分隔
        int idx = url.indexOf(';');
        char splitChar = ';';
        if (idx < 0) {
            //看看是否是MySQL的JDBC URL语法:
            //jdbc:mysql://[host:port],[host:port].../[database][?propertyName1][=propertyValue1][&propertyName2][=propertyValue2]
            //数据库名与参数之间用'?'号分隔，不同参数之间用'&'分隔
            idx = url.indexOf('?');
            if (idx >= 0)
                splitChar = '&';
        }
        if (idx >= 0) {
            String settings = url.substring(idx + 1);
            url = url.substring(0, idx); //去掉后面的参数
            String[] list = StringUtils.arraySplit(settings, splitChar, false);
            for (String setting : list) {
                if (setting.length() == 0) {
                    continue;
                }
                int equal = setting.indexOf('=');
                if (equal < 0) {
                    throw getFormatException();
                }
                String value = setting.substring(equal + 1);
                String key = setting.substring(0, equal);
                key = StringUtils.toUpperEnglish(key);
                if (!isKnownSetting(key) && !dbSettings.containsKey(key)) {
                    throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, key);
                }
                String old = prop.getProperty(key);
                if (old != null && !old.equals(value)) {
                    throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
                }
                prop.setProperty(key, value);
            }
        }
    }

    private char[] removePassword() {
        Object p = prop.remove("PASSWORD");
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
        boolean passwordHash = removeProperty("PASSWORD_HASH", false);
        if (getProperty("CIPHER", null) != null) {
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
            filePasswordHash = hashPassword(passwordHash, "file", filePassword);
        }
        userPasswordHash = hashPassword(passwordHash, user, password);
    }

    private static byte[] hashPassword(boolean passwordHash, String userName, char[] password) {
        if (passwordHash) {
            return StringUtils.convertHexToBytes(new String(password));
        }
        if (userName.length() == 0 && password.length == 0) {
            return new byte[0];
        }
        return SHA256.getKeyPasswordHash(userName, password);
    }

    /**
     * Get a boolean property if it is set and return the value.
     *
     * @param key the property name
     * @param defaultValue the default value
     * @return the value
     */
    boolean getProperty(String key, boolean defaultValue) {
        String x = getProperty(key, null);
        if (x == null) {
            return defaultValue;
        }
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
        return x == null ? defaultValue : Boolean.parseBoolean(x);
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

    public String getDatabaseName() {
        return dbName;
    }

    /**
     * Get the file password hash if it is set.
     *
     * @return the password hash or null
     */
    byte[] getFilePasswordHash() {
        return filePasswordHash;
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
    byte[] getUserPasswordHash() {
        return userPasswordHash;
    }

    /**
     * Get the property keys.
     *
     * @return the property keys
     */
    String[] getKeys() {
        String[] keys = new String[prop.size()];
        prop.keySet().toArray(keys);
        return keys;
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

    /**
     * Get the value of the given property.
     *
     * @param setting the setting id
     * @param defaultValue the default value
     * @return the value as a String
     */
    String getProperty(int setting, String defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key);
        return s == null ? defaultValue : s;
    }

    /**
     * Get the value of the given property.
     *
     * @param setting the setting id
     * @param defaultValue the default value
     * @return the value as an integer
     */
    int getIntProperty(int setting, int defaultValue) {
        String key = SetTypes.getTypeName(setting);
        String s = getProperty(key, null);
        try {
            return s == null ? defaultValue : Integer.decode(s);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Check if this is a remote connection with SSL enabled.
     *
     * @return true if it is
     */
    boolean isSSL() {
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
    DbException getFormatException() {
        return DbException.get(ErrorCode.URL_FORMAT_ERROR_2, Constants.URL_FORMAT, url);
    }

    /**
     * Switch to server mode, and set the server name and database key.
     *
     * @param serverKey the server name, '/', and the security key
     */
    public void setServerKey(String serverKey) {
        remote = true;
        dbName = serverKey;
    }

    public DbSettings getDbSettings() {
        DbSettings defaultSettings = DbSettings.getInstance();
        HashMap<String, String> s = null;
        for (Object k : prop.keySet()) {
            String key = k.toString();
            if (!isKnownSetting(key) && defaultSettings.containsKey(key)) {
                if (s == null) {
                    s = New.hashMap();
                }
                s.put(key, prop.getProperty(key));
            }
        }
        return DbSettings.getInstance(s);
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

    public SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            try {
                sessionFactory = (SessionFactory) Class.forName("org.lealone.engine.DatabaseEngine").getMethod("getInstance")
                        .invoke(null);
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        return sessionFactory;
    }

    public boolean isDynamic() {
        return dynamic;
    }

    public void setSession(SessionInterface session) {
        this.session = session;
    }

    public SessionInterface getSession() {
        return session;
    }

}
