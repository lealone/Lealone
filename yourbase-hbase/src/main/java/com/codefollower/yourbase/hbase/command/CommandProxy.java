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
    private CommandInterface proxyCommand;
    private Prepared originalPrepared;
    private ArrayList<? extends ParameterInterface> originalParams;

    /**
     * SQL是否是参数化的？
     * 对于PreparedStatement(或其子类)，因为存在未知参数，在prepared阶段还没有参数值，
     * 所以此时rowKey字段还没有值，无法确定当前SQL要放到哪里执行，
     * 只能推迟到executeQuery和executeUpdate时再解析rowKey
     */
    private boolean isParameterized;

    public CommandProxy(Parser parser, String sql, Command originalCommand) {
        super(parser, sql);
        this.session = (HBaseSession) parser.getSession();
        originalPrepared = originalCommand.getPrepared();
        originalParams = originalCommand.getParameters();

        isParameterized = originalParams != null && !originalParams.isEmpty();

        if (isParameterized) {
            this.proxyCommand = originalCommand; //对于参数化的SQL，推迟到executeQuery和executeUpdate时再解析rowKey
        } else {
            parseRowKey(); //不带参数时直接解析rowKey
        }
    }

    private CommandInterface getCommandInterface(String url, String sql) throws Exception {
        return getCommandInterface(new Properties(session.getOriginalProperties()), url, sql);
    }

    CommandInterface getCommandInterface(Properties info, String url, String sql) throws Exception {
        return getCommandInterface(session, info, url, sql, originalParams); //传递最初的参数值到新的CommandInterface
    }

    private void parseRowKey() {
        Command originalCommand = originalPrepared.getCommand();
        try {
            //1. DDL类型的SQL全转向Master处理
            if (originalPrepared instanceof DefineCommand) {
                if (session.getMaster() != null) {
                    this.proxyCommand = originalCommand;
                } else if (session.getRegionServer() != null) {
                    ServerName msn = HBaseUtils.getMasterServerName();
                    ServerName rsn = session.getRegionServer().getServerName();
                    if (ZooKeeperAdmin.getTcpPort(msn) == ZooKeeperAdmin.getTcpPort(rsn)
                            && msn.getHostname().equalsIgnoreCase(rsn.getHostname())) {
                        this.proxyCommand = originalCommand;
                    } else {
                        this.proxyCommand = getCommandInterface(HBaseUtils.getMasterURL(), sql);
                    }
                } else {
                    this.proxyCommand = getCommandInterface(HBaseUtils.getMasterURL(), sql);
                }

                //2. 如果SQL不是分布式的，那么直接在本地执行
            } else if (!originalPrepared.isDistributedSQL()) {
                this.proxyCommand = originalCommand;

                //3. 如果SQL是HBasePrepared类型，需要进一步判断
            } else if (originalPrepared instanceof HBasePrepared) {
                HBasePrepared hp = (HBasePrepared) originalPrepared;

                if (originalPrepared instanceof Insert) {
                    String tableName = originalPrepared.getTableName();
                    String rowKey = originalPrepared.getRowKey();
                    if (rowKey == null)
                        throw new RuntimeException("rowKey is null");

                    HBaseRegionInfo hri = HBaseUtils.getHBaseRegionInfo(tableName, rowKey);
                    if (isLocal(session, hri)) {
                        hp.setRegionName(hri.getRegionName());
                        this.proxyCommand = originalCommand;
                    } else {
                        this.proxyCommand = getCommandInterface(session.getOriginalProperties(), hri.getRegionServerURL(),
                                createSQL(hri.getRegionName(), sql));
                    }
                } else if (originalPrepared instanceof Delete || originalPrepared instanceof Update) {
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

                    byte[] tableName = Bytes.toBytes(originalPrepared.getTableName());
                    boolean oneRegion = false;
                    List<byte[]> startKeys = null;
                    if (startValue != null && endValue != null && startValue == endValue)
                        oneRegion = true;

                    if (!oneRegion) {
                        startKeys = HBaseUtils.getStartKeysInRange(tableName, start, end);
                        if (startKeys == null || startKeys.isEmpty()) {
                            this.proxyCommand = originalCommand; //TODO 找不到任何Region时说明此时Delete或Update都无效果
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
                            this.proxyCommand = originalCommand;
                        } else {
                            //Properties info = new Properties(session.getOriginalProperties());
                            //info.setProperty("REGION_NAME", hri.getRegionName());
                            this.proxyCommand = getCommandInterface(session.getOriginalProperties(), hri.getRegionServerURL(),
                                    createSQL(hri.getRegionName(), sql));
                        }
                    } else {
                        this.proxyCommand = new CommandParallel(session, this, tableName, startKeys, sql);
                    }
                }
            } else if (originalPrepared instanceof Select) {
                byte[] startRowKey = null;
                byte[] stopRowKey = null;
                String[] rowKeys = originalPrepared.getRowKeys();
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
                c.select = (Select) originalPrepared;
                c.tableName = Bytes.toBytes(originalPrepared.getTableName());
                c.start = startRowKey;
                c.end = stopRowKey;
                c.sql = sql;
                c.fetchSize = session.getFetchSize();
                c.session = session;
                this.proxyCommand = c.init();
            } else {
                this.proxyCommand = originalCommand;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setProxyCommandParameters() {
        //当Command是在本地执行时，proxyCommand.getParameters()就是originalParams，此时不需要重复设置
        if (originalParams != null && proxyCommand.getParameters() != null && proxyCommand.getParameters() != originalParams) {
            ArrayList<? extends ParameterInterface> params = proxyCommand.getParameters();
            for (int i = 0, size = params.size(); i < size; i++) {
                params.get(i).setValue(originalParams.get(i).getParamValue(), true);
            }
        }
    }

    @Override
    public ResultInterface executeQuery(int maxrows, boolean scrollable) {
        //TcpServerThread在处理COMMAND_EXECUTE_QUERY和COMMAND_EXECUTE_UPDATE时，
        //如果存在参数，则在setParameters方法中调用Command.getParameters()为每个Parameter赋值，
        //所以如果是参数化的SQL，则需要解析rowKey。
        if (isParameterized) {
            parseRowKey();
        }
        setProxyCommandParameters();
        return proxyCommand.executeQuery(maxrows, scrollable);
    }

    @Override
    public int executeUpdate() {
        if (isParameterized) {
            parseRowKey();
        }
        setProxyCommandParameters();
        int updateCount = proxyCommand.executeUpdate();
        if (!session.getDatabase().isMaster() && originalPrepared instanceof DefineCommand) {
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
        return originalPrepared.getType();
    }

    @Override
    public boolean isTransactional() {
        return originalPrepared.isTransactional();
    }

    @Override
    public boolean isQuery() {
        return originalPrepared.isQuery();
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return originalParams;
    }

    @Override
    public boolean isReadOnly() {
        return originalPrepared.isReadOnly();
    }

    @Override
    public ResultInterface queryMeta() {
        return originalPrepared.queryMeta();
    }

    @Override
    public Prepared getPrepared() {
        return originalPrepared;
    }

    @Override
    public boolean isCacheable() {
        return originalPrepared.isCacheable();
    }

    @Override
    public void close() {
        proxyCommand.close();
        super.close();
    }

    @Override
    public void cancel() {
        proxyCommand.cancel();
        super.cancel();
    }

    public static String createSQL(String regionName, String sql) {
        StringBuilder buff = new StringBuilder("IN THE REGION ");
        buff.append(StringUtils.quoteStringSQL(regionName)).append(" ").append(sql);
        return buff.toString();
    }

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

}
