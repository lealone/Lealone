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
package com.codefollower.lealone.hbase.command;

import java.util.ArrayList;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.expression.Parameter;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.engine.SessionRemotePool;
import com.codefollower.lealone.result.ResultInterface;

//只重写了update、query两个方法
public class MasterDDLPrepared extends Prepared {
    private final HBaseSession session;
    private final Prepared delegate;
    private final String sql;

    public MasterDDLPrepared(Session session, Prepared delegate, String sql) {
        super(session);
        this.session = (HBaseSession) session;
        this.delegate = delegate;
        this.sql = sql;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean isTransactional() {
        return delegate.isTransactional();
    }

    @Override
    public ResultInterface queryMeta() {
        return delegate.queryMeta();
    }

    @Override
    public int getType() {
        return delegate.getType();
    }

    @Override
    public boolean isReadOnly() {
        return delegate.isReadOnly();
    }

    @Override
    public boolean needRecompile() {
        return delegate.needRecompile();
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public void setParameterList(ArrayList<Parameter> parameters) {
        delegate.setParameterList(parameters);
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return delegate.getParameters();
    }

    @Override
    public void setCommand(Command command) {
        delegate.setCommand(command);
    }

    @Override
    public boolean isQuery() {
        return delegate.isQuery();
    }

    @Override
    public void prepare() {
        delegate.prepare();
    }

    @Override
    public int update() {
        CommandInterface c = SessionRemotePool.getMasterCommand(session.getOriginalProperties(), sql, getParameters());
        try {
            return c.executeUpdate();
        } finally {
            c.close();
        }
    }

    @Override
    public ResultInterface query(int maxRows) {
        CommandInterface c = SessionRemotePool.getMasterCommand(session.getOriginalProperties(), sql, getParameters());
        try {
            return c.executeQuery(maxRows, false);
        } finally {
            c.close();
        }
    }

    @Override
    public void setSQL(String sql) {
        delegate.setSQL(sql);
    }

    @Override
    public String getSQL() {
        return delegate.getSQL();
    }

    @Override
    public String getPlanSQL() {
        return delegate.getPlanSQL();
    }

    @Override
    public void checkCanceled() {
        delegate.checkCanceled();
    }

    @Override
    public void setObjectId(int i) {
        delegate.setObjectId(i);
    }

    @Override
    public void setSession(Session currentSession) {
        delegate.setSession(currentSession);
    }

    @Override
    public void setPrepareAlways(boolean prepareAlways) {
        delegate.setPrepareAlways(prepareAlways);
    }

    @Override
    public int getCurrentRowNumber() {
        return delegate.getCurrentRowNumber();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean isCacheable() {
        return delegate.isCacheable();
    }

    @Override
    public boolean isDistributedSQL() {
        return delegate.isDistributedSQL();
    }

    @Override
    public Command getCommand() {
        return delegate.getCommand();
    }
}
