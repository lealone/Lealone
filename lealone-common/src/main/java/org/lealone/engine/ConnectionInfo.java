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
import org.lealone.fs.FilePathEncrypt;
import org.lealone.fs.FileUtils;
import org.lealone.message.DbException;
import org.lealone.security.SHA256;
import org.lealone.util.New;
import org.lealone.util.SortedProperties;
import org.lealone.util.StringUtils;

/**
 * Encapsulates the connection settings, including user name and password.
 */
public class ConnectionInfo implements Cloneable {
    private static final HashSet<String> KNOWN_SETTINGS = New.hashSet();

    static {
        ArrayList<String> list = SetTypes.getTypes();
        HashSet<String> set = KNOWN_SETTINGS;
        set.addAll(list);
        String[] connectionSettings = { "AUTOCOMMIT", "CIPHER", "CREATE", "CACHE_TYPE", "IGNORE_UNKNOWN_SETTINGS",
                "IFEXISTS", "INIT", "PASSWORD", "RECOVER", "RECOVER_TEST", "USER", "OPEN_NEW", "PAGE_SIZE",
                "PASSWORD_HASH", "IS_LOCAL" };
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

    private final Properties prop = new Properties();
    private String url; //不包含后面的参数
    private String user;
    private byte[] filePasswordHash;
    private byte[] fileEncryptionKey;
    private byte[] userPasswordHash;

    /**
     * The database name
     */
    private String dbName;
    private String nameNormalized;
    private boolean remote;
    private boolean ssl;
    private boolean persistent;
    private boolean embedded;
    private String servers;

    private SessionFactory sessionFactory;
    private DbSettings dbSettings;

    /**
     * Create a server connection info object.
     *  
     * @param url
     * @param dbName
     */
    public ConnectionInfo(String url, String dbName) { //用于server端, 不需要再解析URL了
        this.url = url;
        this.dbName = dbName;

        checkURL();

        persistent = true;
        embedded = false;

        url = url.substring(Constants.URL_PREFIX.length());
        if (url.startsWith(Constants.URL_MEM)) {
            persistent = false;
            url = url.substring(Constants.URL_MEM.length());
        }
        //server端接收到的URL不可能是嵌入式的
        if (url.startsWith(Constants.URL_EMBED)) {
            throw DbException.throwInternalError("Server backend URL: " + this.url);
        }
    }

    public ConnectionInfo(String url) {
        this(url, (Properties) null);
    }

    /**
     * Create a client connection info object.
     *
     * @param url the database URL
     * @param info the connection properties
     */
    public ConnectionInfo(String url, Properties info) { //用于client端，需要解析URL
        this.url = remapURL(url);

        checkURL();
        readProperties(info);
        readAndRemoveSettingsFromURL();
        parseURL();

        setUserName(removeProperty("USER", ""));
        convertPasswords();
    }

    private void checkURL() {
        if (url == null || !url.startsWith(Constants.URL_PREFIX)) {
            throw getFormatException();
        }
    }

    //如果URL中有参数先读出来，然后从URL中移除
    private void readAndRemoveSettingsFromURL() {
        //支持两种风格的JDBC URL参数语法
        //1. MySQL的JDBC URL参数语法:
        //.../database[?propertyName1=propertyValue1][&propertyName2=propertyValue2]
        //数据库名与参数之间用'?'号分隔，不同参数之间用'&'分隔

        //2.Lealone的JDBC URL参数语法:
        //.../database[;propertyName1=propertyValue1][;propertyName2=propertyValue2]
        //数据库名与参数之间用';'号分隔，不同参数之间也用';'号分隔
        int idx = url.indexOf('?');
        char splitChar;
        if (idx >= 0) {
            splitChar = '&';
            if (url.indexOf(';') >= 0)
                throw getFormatException(); //不能同时出现'&'和';'
        } else {
            idx = url.indexOf(';');
            splitChar = ';';
            if (url.indexOf('&') >= 0)
                throw getFormatException(); //不能出现'&'
        }

        if (idx >= 0) {
            DbSettings dbSettings = DbSettings.getDefaultSettings();
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

    private void parseURL() {
        boolean mem = false;
        remote = true;
        dbName = url.substring(Constants.URL_PREFIX.length());

        if (dbName.startsWith(Constants.URL_MEM)) {
            dbName = dbName.substring(Constants.URL_MEM.length());
            mem = true;
        }

        if (dbName.startsWith(Constants.URL_TCP)) {
            dbName = dbName.substring(Constants.URL_TCP.length());
        } else if (dbName.startsWith(Constants.URL_SSL)) {
            ssl = true;
            dbName = dbName.substring(Constants.URL_SSL.length());
        } else if (dbName.startsWith(Constants.URL_EMBED)) {
            remote = false;
            embedded = true;
            dbName = dbName.substring(Constants.URL_EMBED.length());
            if (!mem)
                persistent = true;
        } else {
            throw getFormatException();
        }

        if (remote) {
            if (dbName.startsWith("//")) //在URL中"//"是可选的
                dbName = dbName.substring("//".length());

            int idx = dbName.indexOf('/');
            if (idx < 0)
                throw getFormatException();
            servers = dbName.substring(0, idx);
            dbName = dbName.substring(idx + 1);
        }
    }

    /**
     * Set the base directory of persistent databases, unless the database is in
     * the user home folder (~).
     *
     * @param dir the new base directory
     */
    public void setBaseDir(String dir) {
        if (persistent) {
            String absDir = FileUtils.unwrap(FileUtils.toRealPath(dir));
            boolean absolute = FileUtils.isAbsolute(dbName);
            String n;
            String prefix = null;
            if (dir.endsWith(SysProperties.FILE_SEPARATOR)) {
                dir = dir.substring(0, dir.length() - 1);
            }
            if (absolute) {
                n = dbName;
            } else {
                n = FileUtils.unwrap(dbName);
                prefix = dbName.substring(0, dbName.length() - n.length());
                n = dir + SysProperties.FILE_SEPARATOR + n;
            }
            String normalizedName = FileUtils.unwrap(FileUtils.toRealPath(n));
            if (normalizedName.equals(absDir) || !normalizedName.startsWith(absDir)) {
                // database name matches the baseDir or
                // database name is clearly outside of the baseDir
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, normalizedName + " outside " + absDir);
            }
            if (absDir.endsWith("/") || absDir.endsWith("\\")) {
                // no further checks are needed for C:/ and similar
            } else if (normalizedName.charAt(absDir.length()) != '/') {
                // database must be within the directory
                // (with baseDir=/test, the database name must not be
                // /test2/x and not /test2)
                throw DbException.get(ErrorCode.IO_EXCEPTION_1, normalizedName + " outside " + absDir);
            }
            if (!absolute) {
                dbName = prefix + dir + SysProperties.FILE_SEPARATOR + FileUtils.unwrap(dbName);
            }
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

    /**
     * Check if the referenced database is persistent.
     *
     * @return true if it is
     */
    public boolean isPersistent() {
        return persistent;
    }

    public boolean isEmbedded() {
        return embedded;
    }

    public String getServers() {
        return servers;
    }

    public void readProperties(Properties info) {
        if (info == null)
            return;
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
                    s = DbSettings.getDefaultSettings();
                }
                if (s.containsKey(key)) {
                    prop.put(key, value);
                }
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
            fileEncryptionKey = FilePathEncrypt.getPasswordBytes(filePassword);
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
        if (persistent) {
            String name = dbName;
            if (nameNormalized == null) {
                //                if (!SysProperties.IMPLICIT_RELATIVE_PATH) {
                //                    if (!FileUtils.isAbsolute(name)) {
                //                        if (name.indexOf("./") < 0 && name.indexOf(".\\") < 0 && name.indexOf(":/") < 0
                //                                && name.indexOf(":\\") < 0) {
                //                            // the name could start with "./", or
                //                            // it could start with a prefix such as "nio:./"
                //                            // for Windows, the path "\test" is not considered
                //                            // absolute as the drive letter is missing,
                //                            // but we consider it absolute
                //                            throw DbException.get(ErrorCode.URL_RELATIVE_TO_CWD, url);
                //                        }
                //                    }
                //                }
                String suffix = Constants.SUFFIX_MV_FILE;
                String n = FileUtils.toRealPath(name + suffix);
                String fileName = FileUtils.getName(n);
                if (fileName.length() < suffix.length() + 1) {
                    throw DbException.get(ErrorCode.INVALID_DATABASE_NAME_1, name);
                }
                nameNormalized = n.substring(0, n.length() - suffix.length());
            }
            return nameNormalized;
        }
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

    byte[] getFileEncryptionKey() {
        return fileEncryptionKey;
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

    public void setFileEncryptionKey(byte[] key) {
        this.fileEncryptionKey = key;
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
    private DbException getFormatException() {
        return DbException.get(ErrorCode.URL_FORMAT_ERROR_2, Constants.URL_FORMAT, url);
    }

    public DbSettings getDbSettings() {
        if (dbSettings == null) {
            DbSettings defaultSettings = DbSettings.getDefaultSettings();
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
            dbSettings = DbSettings.getInstance(s);
        }
        return dbSettings;
    }

    public SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            try {
                sessionFactory = (SessionFactory) Class.forName("org.lealone.engine.DatabaseEngine")
                        .getMethod("getInstance").invoke(null);
            } catch (Exception e) {
                throw DbException.convert(e);
            }
        }
        return sessionFactory;
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
}
