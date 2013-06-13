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
package com.codefollower.lealone.hbase.command.dml;

import com.codefollower.lealone.command.Command;
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.CommandParallel;
import com.codefollower.lealone.hbase.command.HBasePrepared;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.result.ResultInterface;

public class InTheRegion extends Prepared {
    private Prepared originalPrepared;
    private String regionName;
    private CommandParallel commandParallel;

    public InTheRegion(Session session, String[] regionNames, Prepared prepared) {
        super(session);
        this.regionName = regionNames[0];
        this.originalPrepared = prepared;

        if (regionNames.length > 1) {
            commandParallel = new CommandParallel((HBaseSession) session, regionNames, prepared);
        }
    }

    public String getRegionName() {
        return regionName;
    }

    public boolean isReadOnly() {
        return originalPrepared.isReadOnly();
    }

    public boolean needRecompile() {
        return originalPrepared.needRecompile();
    }

    public String getPlanSQL() {
        return getSQL();
    }

    public void checkCanceled() {
        originalPrepared.checkCanceled();
    }

    public void setPrepareAlways(boolean prepareAlways) {
        originalPrepared.setPrepareAlways(prepareAlways);
    }

    public int getCurrentRowNumber() {
        return originalPrepared.getCurrentRowNumber();
    }

    public boolean isCacheable() {
        return originalPrepared.isCacheable();
    }

    public boolean isDistributedSQL() {
        return false; //只在本机执行
    }

    @Override
    public boolean isTransactional() {
        return originalPrepared.isTransactional();
    }

    @Override
    public ResultInterface queryMeta() {
        return originalPrepared.queryMeta();
    }

    @Override
    public int getType() {
        return originalPrepared.getType();
    }

    @Override
    public boolean isQuery() {
        return originalPrepared.isQuery();
    }

    @Override
    public void prepare() {
        if (originalPrepared instanceof HBasePrepared)
            ((HBasePrepared) originalPrepared).setRegionName(regionName);
        originalPrepared.prepare();
    }

    @Override
    public int update() {
        if (commandParallel != null)
            return commandParallel.executeUpdate();
        else
            return originalPrepared.update();
    }

    @Override
    public ResultInterface query(int maxrows) {
        if (commandParallel != null)
            return commandParallel.executeQuery(maxrows, false);
        else
            return originalPrepared.query(maxrows);
    }

    public void setCommand(Command command) {
        super.setCommand(command);
        originalPrepared.setCommand(command);
    }

    @Override
    public Command getCommand() {
        return originalPrepared.getCommand();
    }
}
