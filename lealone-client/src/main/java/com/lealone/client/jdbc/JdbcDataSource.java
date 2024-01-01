/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.client.jdbc;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import com.lealone.common.trace.TraceObjectType;
import com.lealone.common.util.StringUtils;

/**
 * A data source for Lealone database connections.
 * This class is usually registered in a JNDI naming service. 
 * To create a data source object and register it with a JNDI service,
 * use the following code:
 *
 * <pre>
 * import com.lealone.client.jdbcx.JdbcDataSource;
 * import javax.naming.Context;
 * import javax.naming.InitialContext;
 * JdbcDataSource ds = new JdbcDataSource();
 * ds.setURL(&quot;jdbc:lealone:&tilde;/test&quot;);
 * ds.setUser(&quot;sa&quot;);
 * ds.setPassword(&quot;sa&quot;);
 * Context ctx = new InitialContext();
 * ctx.bind(&quot;jdbc/dsName&quot;, ds);
 * </pre>
 *
 * To use a data source that is already registered, use the following code:
 *
 * <pre>
 * import java.sql.Connection;
 * import javax.sql.DataSource;
 * import javax.naming.Context;
 * import javax.naming.InitialContext;
 * Context ctx = new InitialContext();
 * DataSource ds = (DataSource) ctx.lookup(&quot;jdbc/dsName&quot;);
 * Connection conn = ds.getConnection();
 * </pre>
 *
 * In this example the user name and password are serialized as
 * well; this may be a security problem in some cases.
 * 
 * @author H2 Group
 * @author zhh
 */
public class JdbcDataSource extends JdbcWrapper
        implements DataSource, Serializable, Referenceable, XADataSource {

    private static final long serialVersionUID = 1288136338451857771L;

    private transient JdbcDataSourceFactory factory;
    private transient PrintWriter logWriter;
    private int loginTimeout;
    private String userName = "";
    private char[] passwordChars = {};
    private String url = "";
    private String description;

    static {
        JdbcDriver.load();
    }

    /**
     * The public constructor.
     */
    public JdbcDataSource() {
        initFactory();
        int id = getNextTraceId(TraceObjectType.DATA_SOURCE);
        this.trace = factory.getTrace(TraceObjectType.DATA_SOURCE, id);
    }

    /**
     * Called when de-serializing the object.
     *
     * @param in the input stream
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        initFactory();
        in.defaultReadObject();
    }

    private void initFactory() {
        factory = new JdbcDataSourceFactory();
    }

    /**
     * Get the login timeout in seconds, 0 meaning no timeout.
     *
     * @return the timeout in seconds
     */
    @Override
    public int getLoginTimeout() {
        debugCodeCall("getLoginTimeout");
        return loginTimeout;
    }

    /**
     * Set the login timeout in seconds, 0 meaning no timeout.
     * The default value is 0.
     * This value is ignored by this database.
     *
     * @param timeout the timeout in seconds
     */
    @Override
    public void setLoginTimeout(int timeout) {
        debugCodeCall("setLoginTimeout", timeout);
        this.loginTimeout = timeout;
    }

    /**
     * Get the current log writer for this object.
     *
     * @return the log writer
     */
    @Override
    public PrintWriter getLogWriter() {
        debugCodeCall("getLogWriter");
        return logWriter;
    }

    /**
     * Set the current log writer for this object.
     * This value is ignored by this database.
     *
     * @param out the log writer
     */
    @Override
    public void setLogWriter(PrintWriter out) {
        debugCodeCall("setLogWriter(out)");
        logWriter = out;
    }

    /**
     * Open a new connection using the current URL, user name and password.
     *
     * @return the connection
     */
    @Override
    public Connection getConnection() throws SQLException {
        debugCodeCall("getConnection");
        return getJdbcConnection(userName, StringUtils.cloneCharArray(passwordChars));
    }

    /**
     * Open a new connection using the current URL and the specified user name
     * and password.
     *
     * @param user the user name
     * @param password the password
     * @return the connection
     */
    @Override
    public Connection getConnection(String user, String password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getConnection(" + quote(user) + ", \"\");");
        }
        return getJdbcConnection(user, convertToCharArray(password));
    }

    private JdbcConnection getJdbcConnection(String user, char[] password) throws SQLException {
        if (isDebugEnabled()) {
            debugCode("getJdbcConnection(" + quote(user) + ", new char[0]);");
        }
        Properties info = new Properties();
        info.setProperty("user", user);
        info.put("password", convertToString(password));
        Connection conn = JdbcDriver.load().connect(url, info);
        if (conn == null) {
            throw new SQLException("No suitable driver found for " + url, "08001", 8001);
        } else if (!(conn instanceof JdbcConnection)) {
            throw new SQLException("Connecting with old version is not supported: " + url, "08001",
                    8001);
        }
        return (JdbcConnection) conn;
    }

    /**
     * Get the current URL.
     *
     * @return the URL
     */
    public String getURL() {
        debugCodeCall("getURL");
        return url;
    }

    /**
     * Set the current URL.
     *
     * @param url the new URL
     */
    public void setURL(String url) {
        debugCodeCall("setURL", url);
        this.url = url;
    }

    /**
     * Set the current password.
     *
     * @param password the new password.
     */
    public void setPassword(String password) {
        debugCodeCall("setPassword", "");
        this.passwordChars = convertToCharArray(password);
    }

    /**
     * Set the current password in the form of a char array.
     *
     * @param password the new password in the form of a char array.
     */
    public void setPasswordChars(char[] password) {
        if (isDebugEnabled()) {
            debugCode("setPasswordChars(new char[0]);");
        }
        this.passwordChars = password;
    }

    private static char[] convertToCharArray(String s) {
        return s == null ? null : s.toCharArray();
    }

    private static String convertToString(char[] a) {
        return a == null ? null : new String(a);
    }

    /**
     * Get the current password.
     *
     * @return the password
     */
    public String getPassword() {
        debugCodeCall("getPassword");
        return convertToString(passwordChars);
    }

    /**
     * Get the current user name.
     *
     * @return the user name
     */
    public String getUser() {
        debugCodeCall("getUser");
        return userName;
    }

    /**
     * Set the current user name.
     *
     * @param user the new user name
     */
    public void setUser(String user) {
        debugCodeCall("setUser", user);
        this.userName = user;
    }

    /**
     * Get the current description.
     *
     * @return the description
     */
    public String getDescription() {
        debugCodeCall("getDescription");
        return description;
    }

    /**
     * Set the description.
     *
     * @param description the new description
     */
    public void setDescription(String description) {
        debugCodeCall("getDescription", description);
        this.description = description;
    }

    /**
     * Get a new reference for this object, using the current settings.
     *
     * @return the new reference
     */
    @Override
    public Reference getReference() {
        debugCodeCall("getReference");
        String factoryClassName = JdbcDataSourceFactory.class.getName();
        Reference ref = new Reference(getClass().getName(), factoryClassName, null);
        ref.add(new StringRefAddr("url", url));
        ref.add(new StringRefAddr("user", userName));
        ref.add(new StringRefAddr("password", convertToString(passwordChars)));
        ref.add(new StringRefAddr("loginTimeout", String.valueOf(loginTimeout)));
        ref.add(new StringRefAddr("description", description));
        return ref;
    }

    /**
     * [Not supported]
     */
    // ## Java 1.7 ##
    @Override
    public Logger getParentLogger() {
        return null;
    }

    /**
     * INTERNAL
     */
    @Override
    public String toString() {
        return getTraceObjectName() + ": url=" + url + " user=" + userName;
    }

    @Override
    public XAConnection getXAConnection() throws SQLException {
        return null;
    }

    @Override
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        return null;
    }
}
