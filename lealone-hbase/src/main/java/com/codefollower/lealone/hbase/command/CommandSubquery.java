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
import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.command.dml.Query;
import com.codefollower.lealone.constant.SysProperties;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.result.ResultInterface;

public class CommandSubquery extends Command {
    private final Query query;

    public CommandSubquery(Session session, Query query) {
        super(session, query.getSQL());
        this.query = query;
        query.setCommand(this);
        setFetchSize(SysProperties.SERVER_RESULT_SET_FETCH_SIZE);
    }

    public ResultInterface query(int maxrows) {
        return query.query(maxrows);
    }

    @Override
    public int getCommandType() {
        return query.getType();
    }

    @Override
    public boolean isTransactional() {
        return query.isTransactional();
    }

    @Override
    public boolean isQuery() {
        return true;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return query.getParameters();
    }

    @Override
    public boolean isReadOnly() {
        return query.isReadOnly();
    }

    @Override
    public ResultInterface queryMeta() {
        return query.queryMeta();
    }

    @Override
    public Prepared getPrepared() {
        return query;
    }

}
