/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.h2.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.command.Prepared;
import org.h2.command.ddl.DefineCommand;
import org.h2.command.dml.Delete;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Select;
import org.h2.command.dml.Update;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Engine;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.util.HBaseUtils;

//TODO 目前未使用，目的是想不修改H2的JDBC实现
public class HBaseJdbcConnection implements Connection {
    private final Session session;
    private final ConnectionInfo connectionInfo;
    private final Properties info;
    private final Configuration conf;
    private final HConnection hConnection;

    private JdbcConnection conn;
    private List<JdbcConnection> conns = new ArrayList<JdbcConnection>();

    private HRegionInfo regionInfo;
    private byte[] stopRowKey;
    private byte[] tableName;
    private boolean isGroupQuery = false;

    public byte[] getStopRowKey() {
        return stopRowKey;
    }

    public HRegionInfo getRegionInfo() {
        return regionInfo;
    }

    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    public HBaseJdbcConnection(String url, Properties info) throws SQLException {
        this.info = info;
        connectionInfo = new ConnectionInfo(url, info);
        try {
            connectionInfo.setProperty("OPEN_NEW", "true");
            session = Engine.getInstance().createSession(connectionInfo);
            connectionInfo.removeProperty("OPEN_NEW", false);

            conf = HBaseConfiguration.create();
            hConnection = HConnectionManager.createConnection(conf);
        } catch (Exception re) {
            throw DbException.convert(re);
        }
    }

    private void prepareCommand(String sql) {
        Prepared prepared = session.prepare(sql, true);
        if (prepared instanceof DefineCommand) {
            try {
                Properties info = new Properties(this.info);
                conn = new JdbcConnection(HBaseUtils.getMasterURL(), info);
                conns.add(conn);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (prepared instanceof Insert || prepared instanceof Delete || prepared instanceof Update) {
            String tableName = prepared.getTableName();
            String rowKey = prepared.getRowKey();
            if (rowKey == null)
                throw new RuntimeException("rowKey is null");

            try {
                conn = newJdbcConnection(Bytes.toBytes(tableName), Bytes.toBytes(rowKey));
                conns.add(conn);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (prepared instanceof Select) {
            byte[] startRowKey = null;
            byte[] stopRowKey = null;
            String[] rowKeys = prepared.getRowKeys();
            if (rowKeys != null) {
                if (rowKeys.length >= 1 && rowKeys[0] != null)
                    startRowKey = Bytes.toBytes(rowKeys[0]);

                if (rowKeys.length >= 2 && rowKeys[1] != null)
                    stopRowKey = Bytes.toBytes(rowKeys[1]);
            }

            if (startRowKey == null)
                startRowKey = HConstants.EMPTY_START_ROW;
            if (stopRowKey == null)
                stopRowKey = HConstants.EMPTY_END_ROW;

            this.stopRowKey = stopRowKey;
            this.tableName = Bytes.toBytes(prepared.getTableName());

            this.isGroupQuery = ((Select) prepared).isGroupQuery();
            HBaseHTableInfo hTable = new HBaseHTableInfo();
            hTable.conn = this;
            hTable.conf = conf;
            hTable.tableName = tableName;
            hTable.start = startRowKey;
            hTable.end = stopRowKey;
            hTable.isGroupQuery = this.isGroupQuery;

            conns = hTable.getJdbcConnections();
            conn = conns.get(0);
        }
    }

    private String createUrl(HRegionLocation regionLocation) {
        return createUrl(regionLocation.getHostname(), regionLocation.getH2TcpPort());
    }

    private String createUrl(String hostname, int port) {
        // String url = "jdbc:h2:tcp://" + regionLocation.getHostname() + ":" +
        // "regionLocation.getH2TcpPort() + "/hbasedb";//;disableCheck=true
        StringBuilder url = new StringBuilder(50);
        url.append("jdbc:h2:tcp://").append(hostname).append(":").append(port).append("/hbasedb");
        return url.toString();
    }

    JdbcConnection newJdbcConnection(byte[] tableName, byte[] startKey) {
        try {
            HRegionLocation regionLocation = hConnection.locateRegion(tableName, startKey);
            String url = createUrl(regionLocation);
            Properties info = new Properties(this.info);
            info.setProperty("REGION_NAME", regionLocation.getRegionInfo().getRegionNameAsString());
            return new JdbcConnection(url, info);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    String getNewSQL(Select select, byte[] startKey, boolean isDistributed) {
        return select.getPlanSQL(startKey, stopRowKey, isDistributed);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        return conn.unwrap(iface);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return conn.isWrapperFor(iface);
    }

    public Statement createStatement() throws SQLException {
        return conn.createStatement();
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        prepareCommand(sql);
        return conn.prepareStatement(sql);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return conn.prepareCall(sql);
    }

    public String nativeSQL(String sql) throws SQLException {
        return conn.nativeSQL(sql);
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        conn.setAutoCommit(autoCommit);
    }

    public boolean getAutoCommit() throws SQLException {
        return conn.getAutoCommit();
    }

    public void commit() throws SQLException {
        conn.commit();
    }

    public void rollback() throws SQLException {
        conn.rollback();
    }

    public void close() throws SQLException {
        conn.close();
    }

    public boolean isClosed() throws SQLException {
        return conn.isClosed();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return conn.getMetaData();
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        conn.setReadOnly(readOnly);
    }

    public boolean isReadOnly() throws SQLException {
        return conn.isReadOnly();
    }

    public void setCatalog(String catalog) throws SQLException {
        conn.setCatalog(catalog);
    }

    public String getCatalog() throws SQLException {
        return conn.getCatalog();
    }

    public void setTransactionIsolation(int level) throws SQLException {
        conn.setTransactionIsolation(level);
    }

    public int getTransactionIsolation() throws SQLException {
        return conn.getTransactionIsolation();
    }

    public SQLWarning getWarnings() throws SQLException {
        return conn.getWarnings();
    }

    public void clearWarnings() throws SQLException {
        conn.clearWarnings();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.createStatement(resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return conn.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return conn.getTypeMap();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        conn.setTypeMap(map);
    }

    public void setHoldability(int holdability) throws SQLException {
        conn.setHoldability(holdability);
    }

    public int getHoldability() throws SQLException {
        return conn.getHoldability();
    }

    public Savepoint setSavepoint() throws SQLException {
        return conn.setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return conn.setSavepoint(name);
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        conn.rollback(savepoint);
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        conn.releaseSavepoint(savepoint);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return conn.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return conn.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return conn.prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return conn.prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return conn.prepareStatement(sql, columnNames);
    }

    public Clob createClob() throws SQLException {
        return conn.createClob();
    }

    public Blob createBlob() throws SQLException {
        return conn.createBlob();
    }

    public NClob createNClob() throws SQLException {
        return conn.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return conn.createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return conn.isValid(timeout);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        conn.setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        conn.setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return conn.getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return conn.getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return conn.createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return conn.createStruct(typeName, attributes);
    }

}