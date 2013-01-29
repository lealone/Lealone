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
package com.codefollower.yourbase.hbase.command.dml;

import com.codefollower.yourbase.command.Prepared;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.command.HBasePrepared;
import com.codefollower.yourbase.result.ResultInterface;

public class InTheRegion extends Prepared {
    private Prepared command;
    private String regionName;

    public InTheRegion(Session session, String regionName, Prepared command) {
        super(session);
        this.regionName = regionName;
        this.command = command;
    }

    public String getRegionName() {
        return regionName;
    }

    public boolean isReadOnly() {
        return command.isReadOnly();
    }

    public boolean needRecompile() {
        return command.needRecompile();
    }

    public String getPlanSQL() {
        return getSQL();
    }

    public void checkCanceled() {
        command.checkCanceled();
    }

    public void setPrepareAlways(boolean prepareAlways) {
        command.setPrepareAlways(prepareAlways);
    }

    public int getCurrentRowNumber() {
        return command.getCurrentRowNumber();
    }


    public boolean isCacheable() {
        return command.isCacheable();
    }

    public boolean isDistributedSQL() {
        return false; //只在本机执行
    }

    @Override
    public boolean isTransactional() {
        return command.isTransactional();
    }

    @Override
    public ResultInterface queryMeta() {
        return command.queryMeta();
    }

    @Override
    public int getType() {
        return command.getType();
    }

    @Override
    public boolean isQuery() {
        return command.isQuery();
    }

    @Override
    public void prepare() {
        if (command instanceof HBasePrepared)
            ((HBasePrepared) command).setRegionName(regionName);
        command.prepare();
    }

    @Override
    public int update() {
        return command.update();
    }

    @Override
    public ResultInterface query(int maxrows) {
        return command.query(maxrows);
    }
}
