/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.jmx;

import java.lang.management.ManagementFactory;

import java.sql.Timestamp;
import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.dbobject.table.Table;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Constants;
import com.codefollower.yourbase.engine.Database;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.util.New;

/**
 * The MBean implementation.
 *
 * @author Eric Dong
 * @author Thomas Mueller
 */
public class DatabaseInfo implements DatabaseInfoMBean {

    private static final Map<String, ObjectName> MBEANS = New.hashMap();

    /** Database. */
    private final Database database;

    private DatabaseInfo(Database database) {
        if (database == null) {
            throw new IllegalArgumentException("Argument 'database' must not be null");
        }
        this.database = database;
    }

    /**
     * Returns a JMX new ObjectName instance.
     *
     * @param name name of the MBean
     * @param path the path
     * @return a new ObjectName instance
     * @throws JMException if the ObjectName could not be created
     */
    private static ObjectName getObjectName(String name, String path) throws JMException {
        name = name.replace(':', '_');
        path = path.replace(':', '_');
        Hashtable<String, String> map = new Hashtable<String, String>();
        map.put("name", name);
        map.put("path", path);
        return new ObjectName("com.codefollower.yourbase", map);
    }

    /**
     * Registers an MBean for the database.
     *
     * @param connectionInfo connection info
     * @param database database
     */
    public static void registerMBean(ConnectionInfo connectionInfo, Database database) throws JMException {
        String path = connectionInfo.getName();
        if (!MBEANS.containsKey(path)) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            String name = database.getShortName();
            ObjectName mbeanObjectName = getObjectName(name, path);
            MBEANS.put(path, mbeanObjectName);
            DatabaseInfo info = new DatabaseInfo(database);
            Object mbean = new DocumentedMBean(info, DatabaseInfoMBean.class);
            mbeanServer.registerMBean(mbean, mbeanObjectName);
        }
    }

    /**
     * Unregisters the MBean for the database if one is registered.
     *
     * @param name database name
     */
    public static void unregisterMBean(String name) throws Exception {
        ObjectName mbeanObjectName = MBEANS.remove(name);
        if (mbeanObjectName != null) {
            MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
            mbeanServer.unregisterMBean(mbeanObjectName);
        }
    }

    public boolean isExclusive() {
        return database.getExclusiveSession() != null;
    }

    public boolean isReadOnly() {
        return database.isReadOnly();
    }

    public String getMode() {
        return database.getMode().getName();
    }

    public boolean isMultiThreaded() {
        return database.isMultiThreaded();
    }

    public boolean isMvcc() {
        return database.isMultiVersion();
    }

    public int getLogMode() {
        return database.getLogMode();
    }

    public void setLogMode(int value) {
        database.setLogMode(value);
    }

    public int getTraceLevel() {
        return database.getTraceSystem().getLevelFile();
    }

    public void setTraceLevel(int level) {
        database.getTraceSystem().setLevelFile(level);
    }

    public long getFileWriteCountTotal() {
        return database.getFileWriteCountTotal();
    }

    public long getFileWriteCount() {
        return database.getFileWriteCount();
    }

    public long getFileReadCount() {
        return database.getFileReadCount();
    }

    public long getFileSize() {
        return database.getFileSize();
    }

    public int getCacheSizeMax() {
        return database.getCacheSizeMax();
    }

    public void setCacheSizeMax(int kb) {
        database.setCacheSizeMax(kb);
    }

    public int getCacheSize() {
        return database.getCacheSize();
    }

    public String getVersion() {
        return Constants.getFullVersion();
    }

    public String listSettings() {
        StringBuilder buff = new StringBuilder();
        for (Map.Entry<String, String> e : new TreeMap<String, String>(database.getSettings().getSettings()).entrySet()) {
            buff.append(e.getKey()).append(" = ").append(e.getValue()).append('\n');
        }
        return buff.toString();
    }

    public String listSessions() {
        StringBuilder buff = new StringBuilder();
        for (Session session : database.getSessions(false)) {
            buff.append("session id: ").append(session.getId());
            buff.append(" user: ").append(session.getUser().getName()).append('\n');
            buff.append("connected: ").append(new Timestamp(session.getSessionStart())).append('\n');
            Command command = session.getCurrentCommand();
            if (command != null) {
                buff.append("statement: ").append(session.getCurrentCommand()).append('\n');
                long commandStart = session.getCurrentCommandStart();
                if (commandStart != 0) {
                    buff.append("started: ").append(new Timestamp(commandStart)).append('\n');
                }
            }
            Table[] t = session.getLocks();
            if (t.length > 0) {
                for (Table table : session.getLocks()) {
                    if (table.isLockedExclusivelyBy(session)) {
                        buff.append("write lock on ");
                    } else {
                        buff.append("read lock on ");
                    }
                    buff.append(table.getSchema().getName()).append('.').append(table.getName()).append('\n');
                }
            }
            buff.append('\n');
        }
        return buff.toString();
    }

}
