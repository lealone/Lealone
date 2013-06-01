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
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.expression.ParameterInterface;
import com.codefollower.lealone.result.ResultInterface;
import com.codefollower.lealone.transaction.DistributedTransaction;

class CommandWrapper implements CommandInterface {
    private Command c;
    private Session session;

    CommandWrapper(Command c, Session session) {
        this.c = c;
        this.session = session;
    }

    @Override
    public int getCommandType() {
        return c.getCommandType();
    }

    @Override
    public boolean isQuery() {
        return c.isQuery();
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return c.getParameters();
    }

    @Override
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        return c.executeQuery(maxRows, scrollable);
    }

    @Override
    public int executeUpdate() {
        return c.executeUpdate();
    }

    @Override
    public void close() {
        session.close();
        c.close();
    }

    @Override
    public void cancel() {
        c.cancel();
    }

    @Override
    public ResultInterface getMetaData() {
        return c.getMetaData();
    }

    @Override
    public int getFetchSize() {
        return c.getFetchSize();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        c.setFetchSize(fetchSize);
    }

    @Override
    public void setDistributedTransaction(DistributedTransaction dt) {
        c.setDistributedTransaction(dt);
    }

    @Override
    public DistributedTransaction getDistributedTransaction() {
        return c.getDistributedTransaction();
    }

    @Override
    public void commitDistributedTransaction() {
        c.commitDistributedTransaction();
    }

    @Override
    public void rollbackDistributedTransaction() {
        c.rollbackDistributedTransaction();
    }

}