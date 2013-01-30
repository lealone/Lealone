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

import java.util.ArrayList;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.expression.ParameterInterface;
import com.codefollower.yourbase.result.ResultInterface;

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
}