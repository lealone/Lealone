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
package com.codefollower.lealone.hbase.command.ddl;

import java.io.IOException;
import java.util.ArrayList;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.ddl.DefineCommand;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.expression.Parameter;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.engine.SessionRemotePool;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;

//只重写了update、query两个方法
public class DefineCommandWrapper extends DefineCommand {
    private final HBaseSession session;
    private final DefineCommand dc;
    private final String sql;

    public DefineCommandWrapper(Session session, DefineCommand dc, String sql) {
        super(session);
        this.session = (HBaseSession) session;
        this.dc = dc;
        this.sql = sql;
    }

    @Override
    public int update() {
        CommandInterface c = SessionRemotePool.getMasterCommandRemote(session.getOriginalProperties(), sql, getParameters());
        try {
            return c.executeUpdate();
        } finally {
            c.close();
            refreshMetaTable();
        }
    }

    @Override
    public ResultInterface query(int maxRows) {
        CommandInterface c = SessionRemotePool.getMasterCommandRemote(session.getOriginalProperties(), sql, getParameters());
        try {
            return c.executeQuery(maxRows, false);
        } finally {
            c.close();
            refreshMetaTable();
        }
    }

    private void refreshMetaTable() {
        session.getDatabase().refreshMetaTable();
        try {
            HBaseUtils.reset(); //执行完DDL后，元数据已变动，清除HConnection中的相关缓存
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public boolean isReadOnly() {
        return dc.isReadOnly();
    }

    @Override
    public ResultInterface queryMeta() {
        return dc.queryMeta();
    }

    @Override
    public void setTransactional(boolean transactional) {
        dc.setTransactional(transactional);
    }

    @Override
    public boolean isTransactional() {
        return dc.isTransactional();
    }

    @Override
    public int hashCode() {
        return dc.hashCode();
    }

    @Override
    public int getType() {
        return dc.getType();
    }

    @Override
    public boolean needRecompile() {
        return dc.needRecompile();
    }

    @Override
    public boolean equals(Object obj) {
        return dc.equals(obj);
    }

    @Override
    public void setParameterList(ArrayList<Parameter> parameters) {
        dc.setParameterList(parameters);
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return dc.getParameters();
    }

    @Override
    public void setCommand(Command command) {
        dc.setCommand(command);
    }

    @Override
    public boolean isQuery() {
        return dc.isQuery();
    }

    @Override
    public void prepare() {
        dc.prepare();
    }

    @Override
    public void setSQL(String sql) {
        dc.setSQL(sql);
    }

    @Override
    public String getSQL() {
        return dc.getSQL();
    }

    @Override
    public String getPlanSQL() {
        return dc.getPlanSQL();
    }

    @Override
    public void checkCanceled() {
        dc.checkCanceled();
    }

    @Override
    public void setObjectId(int i) {
        dc.setObjectId(i);
    }

    @Override
    public void setSession(Session currentSession) {
        dc.setSession(currentSession);
    }

    @Override
    public void setPrepareAlways(boolean prepareAlways) {
        dc.setPrepareAlways(prepareAlways);
    }

    @Override
    public int getCurrentRowNumber() {
        return dc.getCurrentRowNumber();
    }

    @Override
    public String toString() {
        return dc.toString();
    }

    @Override
    public boolean isCacheable() {
        return dc.isCacheable();
    }

    @Override
    public Command getCommand() {
        return dc.getCommand();
    }
}
