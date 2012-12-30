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
package org.h2.command;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.command.ddl.DefineCommand;
import org.h2.command.dml.Delete;
import org.h2.command.dml.Insert;
import org.h2.command.dml.Select;
import org.h2.command.dml.Update;
import org.h2.engine.ConnectionInfo;
import org.h2.engine.Session;
import org.h2.engine.SessionInterface;
import org.h2.engine.SessionRemote;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;
import org.h2.util.HBaseRegionInfo;
import org.h2.util.HBaseUtils;
import org.h2.zookeeper.H2MetaTableTracker;

public class CommandProxy extends Command {
    private CommandInterface command;
    private Prepared prepared;
    private ArrayList<? extends ParameterInterface> params;

    public static boolean isLocal(Session session, HBaseRegionInfo hri) throws Exception {
        if (hri == null)
            return false;
        ServerName sn = null;
        if (session.getMaster() != null)
            sn = HBaseUtils.getMasterServerName();
        else if (session.getRegionServer() != null)
            sn = session.getRegionServer().getServerName();
        if (sn == null)
            return false;

        if (hri.getHostname().equalsIgnoreCase(sn.getHostname()) && hri.getH2TcpPort() == sn.getH2TcpPort())
            return true;
        return false;
    }

    //TODO 如何重用SessionInterface
    public static CommandInterface getCommandInterface(Session session, Properties info, String url, String sql,
            List<? extends ParameterInterface> oldParams) throws Exception {

        Properties prop = new Properties();
        for (String key : info.stringPropertyNames())
            prop.setProperty(key, info.getProperty(key));
        ConnectionInfo ci = new ConnectionInfo(url, prop);
        SessionInterface si = new SessionRemote(ci).connectEmbeddedOrServer(false);
        CommandInterface commandInterface = si.prepareCommand(sql, session.getFetchSize());
        ArrayList<? extends ParameterInterface> newParams = commandInterface.getParameters();
        for (int i = 0, size = oldParams.size(); i < size; i++) {
            newParams.get(i).setValue(oldParams.get(i).getParamValue(), true);
        }

        return commandInterface;
    }

    public CommandProxy(Parser parser, String sql, Command command) {
        super(parser, sql);

        prepared = command.getPrepared();
        params = command.getParameters();
        try {
            if (prepared instanceof DefineCommand) {
                if (session.getMaster() != null) {
                    this.command = command;
                } else if (session.getRegionServer() != null) {
                    ServerName msn = HBaseUtils.getMasterServerName();
                    ServerName rsn = session.getRegionServer().getServerName();
                    if (msn.getH2TcpPort() == rsn.getH2TcpPort() && msn.getHostname().equalsIgnoreCase(rsn.getHostname())) {
                        this.command = command;
                    } else {
                        this.command = getCommandInterface(HBaseUtils.getMasterURL(), sql);
                    }
                } else {
                    this.command = getCommandInterface(HBaseUtils.getMasterURL(), sql);
                }
            } else if (!prepared.isDistributedSQL()) {
                this.command = command;
            } else if (prepared instanceof Insert || prepared instanceof Delete || prepared instanceof Update) {
                String tableName = prepared.getTableName();
                String rowKey = prepared.getRowKey();
                if (rowKey == null)
                    throw new RuntimeException("rowKey is null");

                HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, rowKey);
                if (isLocal(session, hri)) {
                    session.setRegionName(Bytes.toBytes(hri.getRegionName()));
                    this.command = command;
                } else {
                    Properties info = new Properties(session.getOriginalProperties());
                    info.setProperty("REGION_NAME", hri.getRegionName());
                    this.command = getCommandInterface(info, hri.getRegionServerURL(), sql);
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

                CommandSelect c = new CommandSelect();
                c.commandProxy = this;
                c.select = (Select) prepared;
                c.tableName = Bytes.toBytes(prepared.getTableName());
                c.start = startRowKey;
                c.end = stopRowKey;
                c.sql = sql;
                c.fetchSize = session.getFetchSize();
                c.session = session;
                this.command = c.init();
            } else {
                this.command = command;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CommandInterface getCommandInterface(String url, String sql) throws Exception {
        return getCommandInterface(new Properties(session.getOriginalProperties()), url, sql);
    }

    CommandInterface getCommandInterface(Properties info, String url, String sql) throws Exception {
        return getCommandInterface(session, info, url, sql, params);
    }

    private void initParams(CommandInterface command) {
        if (params != null && command.getParameters() != null && command.getParameters() != params) {
            ArrayList<? extends ParameterInterface> oldParams = command.getParameters();
            for (int i = 0, size = oldParams.size(); i < size; i++) {
                oldParams.get(i).setValue(params.get(i).getParamValue(), true);
            }
        }
    }

    @Override
    public ResultInterface executeQuery(int maxrows, boolean scrollable) {
        initParams(command);
        return command.executeQuery(maxrows, scrollable);
    }

    @Override
    public int executeUpdate() {
        initParams(command);
        int updateCount = command.executeUpdate();
        if (prepared instanceof DefineCommand) {
            H2MetaTableTracker tracker = session.getDatabase().getH2MetaTableTracker();
            if (tracker != null)
                tracker.updateAll();
        }
        return updateCount;
    }

    @Override
    public int getCommandType() {
        return prepared.getType();
    }

    @Override
    public boolean isTransactional() {
        return prepared.isTransactional();
    }

    @Override
    public boolean isQuery() {
        return prepared.isQuery();
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return params;
    }

    @Override
    public boolean isReadOnly() {
        return prepared.isReadOnly();
    }

    @Override
    public ResultInterface queryMeta() {
        return prepared.queryMeta();
    }

    @Override
    public Prepared getPrepared() {
        return prepared;
    }

    @Override
    public boolean isCacheable() {
        return prepared.isCacheable();
    }

    @Override
    public void close() {
        command.close();
        super.close();
    }

    @Override
    public void cancel() {
        command.cancel();
        super.cancel();
    }
}
