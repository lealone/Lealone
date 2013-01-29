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
package com.codefollower.yourbase.hbase.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Bytes;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.command.Parser;
import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.command.ddl.DefineCommand;
import com.codefollower.yourbase.command.dml.Delete;
import com.codefollower.yourbase.command.dml.Insert;
import com.codefollower.yourbase.command.dml.Select;
import com.codefollower.yourbase.command.dml.Update;
import com.codefollower.yourbase.engine.ConnectionInfo;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.engine.SessionInterface;
import com.codefollower.yourbase.engine.SessionRemote;
import com.codefollower.yourbase.expression.ParameterInterface;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.hbase.util.HBaseRegionInfo;
import com.codefollower.yourbase.hbase.util.HBaseUtils;
import com.codefollower.yourbase.hbase.zookeeper.ZooKeeperAdmin;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.util.StringUtils;
import com.codefollower.yourbase.value.Value;

public class CommandProxy extends Command {
    private final HBaseSession session;
    private CommandInterface command;
    private Prepared prepared;
    private ArrayList<? extends ParameterInterface> params;

    /**
     * 对于PreparedStatement，因为存在未知参数，所以无法确定当前SQL要放到哪里执行
     */
    private boolean isDeterministic;

    public static boolean isLocal(Session s, HBaseRegionInfo hri) throws Exception {
        HBaseSession session = (HBaseSession) s;
        if (hri == null)
            return false;
        ServerName sn = null;
        if (session.getMaster() != null)
            sn = HBaseUtils.getMasterServerName();
        else if (session.getRegionServer() != null)
            sn = session.getRegionServer().getServerName();
        if (sn == null)
            return false;

        if (hri.getHostname().equalsIgnoreCase(sn.getHostname()) && hri.getH2TcpPort() == ZooKeeperAdmin.getTcpPort(sn))
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
        CommandInterface commandInterface = si.prepareCommand(sql, ((HBaseSession) session).getFetchSize());

        if (oldParams != null) {
            ArrayList<? extends ParameterInterface> newParams = commandInterface.getParameters();
            for (int i = 0, size = oldParams.size(); i < size; i++) {
                newParams.get(i).setValue(oldParams.get(i).getParamValue(), true);
            }
        }

        return commandInterface;
    }

    public static SessionInterface getSessionInterface(Session session, Properties info, String url) throws Exception {

        Properties prop = new Properties();
        for (String key : info.stringPropertyNames())
            prop.setProperty(key, info.getProperty(key));
        ConnectionInfo ci = new ConnectionInfo(url, prop);

        return new SessionRemote(ci).connectEmbeddedOrServer(false);
    }

    public CommandProxy(Parser parser, String sql, Command command) {
        super(parser, sql);
        this.session = (HBaseSession) parser.getSession();
        prepared = command.getPrepared();
        params = command.getParameters();

        isDeterministic = params == null || params.isEmpty();

        if (!isDeterministic) {
            this.command = command;
        } else {
            parseRowKey();
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

    //    public String createSQL(String regionName, byte[] start, byte[] end, String sql) {
    //        StringBuilder buff = new StringBuilder("IN THE REGION ");
    //        buff.append(StringUtils.quoteStringSQL(regionName));
    //        if (start != null)
    //            buff.append(" START").append(StringUtils.quoteStringSQL(Bytes.toString(start)));
    //        if (end != null)
    //            buff.append(" END").append(StringUtils.quoteStringSQL(Bytes.toString(end)));
    //        buff.append(" START").append(sql);
    //        return buff.toString();
    //    }

    public String createSQL(String regionName, String sql) {
        StringBuilder buff = new StringBuilder("IN THE REGION ");
        buff.append(StringUtils.quoteStringSQL(regionName)).append(" ").append(sql);
        return buff.toString();
    }

    private void parseRowKey() {
        Command command = prepared.getCommand();
        try {
            if (prepared instanceof DefineCommand) {
                if (session.getMaster() != null) {
                    this.command = command;
                } else if (session.getRegionServer() != null) {
                    ServerName msn = HBaseUtils.getMasterServerName();
                    ServerName rsn = session.getRegionServer().getServerName();
                    if (ZooKeeperAdmin.getTcpPort(msn) == ZooKeeperAdmin.getTcpPort(rsn)
                            && msn.getHostname().equalsIgnoreCase(rsn.getHostname())) {
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
                if (prepared instanceof Delete && prepared instanceof HBasePrepared) {
                    HBasePrepared hp = (HBasePrepared) prepared;
                    byte[] start = null;
                    byte[] end = null;
                    Value startValue = hp.getStartRowKeyValue();
                    Value endValue = hp.getEndRowKeyValue();
                    if (startValue != null)
                        start = HBaseUtils.toBytes(startValue);
                    if (endValue != null)
                        end = HBaseUtils.toBytes(endValue);

                    if (start == null)
                        start = HConstants.EMPTY_START_ROW;
                    if (end == null)
                        end = HConstants.EMPTY_END_ROW;

                    byte[] tableName = Bytes.toBytes(prepared.getTableName());
                    boolean oneRegion = false;
                    List<byte[]> startKeys = null;
                    if (startValue != null && endValue != null && startValue == endValue)
                        oneRegion = true;

                    if (!oneRegion) {
                        startKeys = HBaseUtils.getStartKeysInRange(tableName, start, end);
                        if (startKeys == null || startKeys.isEmpty()) {
                            this.command = command;
                            return;
                        } else if (startKeys.size() == 1) {
                            oneRegion = true;
                            start = startKeys.get(0);
                        }
                    }

                    if (oneRegion) {
                        HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, start);
                        if (CommandProxy.isLocal(session, hri)) {
                            hp.setRegionName(hri.getRegionName());
                            this.command = command;
                        } else {
                            //Properties info = new Properties(session.getOriginalProperties());
                            //info.setProperty("REGION_NAME", hri.getRegionName());
                            this.command = getCommandInterface(session.getOriginalProperties(), hri.getRegionServerURL(),
                                    createSQL(hri.getRegionName(), sql));
                        }
                    } else {
                        this.command = new CommandParallel(session, this, tableName, startKeys, sql);
                    }
                    return;
                }
                String tableName = prepared.getTableName();
                String rowKey = prepared.getRowKey();
                if (rowKey == null)
                    throw new RuntimeException("rowKey is null");
                
                HBasePrepared hp = (HBasePrepared) prepared;

                HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, rowKey);
                if (isLocal(session, hri)) {
                    hp.setRegionName(hri.getRegionName());
                    this.command = command;
                } else {
                    this.command = getCommandInterface(session.getOriginalProperties(), hri.getRegionServerURL(),
                            createSQL(hri.getRegionName(), sql));
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

    @Override
    public ResultInterface executeQuery(int maxrows, boolean scrollable) {
        if (!isDeterministic) {
            parseRowKey();
        }
        initParams(command);
        return command.executeQuery(maxrows, scrollable);
    }

    @Override
    public int executeUpdate() {
        if (!isDeterministic) {
            parseRowKey();
        }
        initParams(command);
        int updateCount = command.executeUpdate();
        if (!session.getDatabase().isMaster() && prepared instanceof DefineCommand) {
            session.getDatabase().refreshMetaTable();

            try {
                HBaseUtils.reset(); //执行完DDL后，元数据已变动，清除HConnection中的相关缓存
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
