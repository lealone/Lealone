/*
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
package org.lealone.sql.ddl;

import java.util.ArrayList;

import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.sql.PreparedStatement;
import org.lealone.sql.expression.Parameter;

//只重写了update方法
public class DefineStatementWrapper extends DefineStatement {
    protected final DefineStatement ds;

    public DefineStatementWrapper(ServerSession session, DefineStatement ds) {
        super(session);
        this.ds = ds;
    }

    @Override
    public int update() {
        return org.lealone.sql.RouterHolder.getRouter().executeDefineCommand(ds);
    }

    @Override
    public int updateLocal() {
        return ds.update();
    }

    @Override
    public boolean isReadOnly() {
        return ds.isReadOnly();
    }

    @Override
    public Result queryMeta() {
        return ds.queryMeta();
    }

    @Override
    public void setTransactional(boolean transactional) {
        ds.setTransactional(transactional);
    }

    @Override
    public boolean isTransactional() {
        return ds.isTransactional();
    }

    @Override
    public int hashCode() {
        return ds.hashCode();
    }

    @Override
    public int getType() {
        return ds.getType();
    }

    @Override
    public boolean needRecompile() {
        return ds.needRecompile();
    }

    @Override
    public boolean equals(Object obj) {
        return ds.equals(obj);
    }

    @Override
    public void setParameterList(ArrayList<Parameter> parameters) {
        ds.setParameterList(parameters);
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return ds.getParameters();
    }

    @Override
    public boolean isQuery() {
        return ds.isQuery();
    }

    @Override
    public PreparedStatement prepare() {
        ds.prepare();
        return this;
    }

    @Override
    public void setSQL(String sql) {
        ds.setSQL(sql);
    }

    @Override
    public String getSQL() {
        return ds.getSQL();
    }

    @Override
    public String getPlanSQL() {
        return ds.getPlanSQL();
    }

    @Override
    public void checkCanceled() {
        ds.checkCanceled();
    }

    @Override
    public void setObjectId(int i) {
        ds.setObjectId(i);
    }

    @Override
    public void setSession(ServerSession currentSession) {
        ds.setSession(currentSession);
    }

    @Override
    public void setPrepareAlways(boolean prepareAlways) {
        ds.setPrepareAlways(prepareAlways);
    }

    @Override
    public int getCurrentRowNumber() {
        return ds.getCurrentRowNumber();
    }

    @Override
    public String toString() {
        return ds.toString();
    }

    @Override
    public boolean isCacheable() {
        return ds.isCacheable();
    }

    @Override
    public void setLocal(boolean local) {
        ds.setLocal(local);
    }

}
