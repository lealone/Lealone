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
package org.lealone.hbase.command;

import java.util.ArrayList;

import org.lealone.api.ParameterInterface;
import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.result.ResultInterface;

class CommandWrapper implements CommandInterface {
    private Prepared p;

    CommandWrapper(Prepared p) {
        this.p = p;
    }

    @Override
    public int getCommandType() {
        return UNKNOWN;
    }

    @Override
    public boolean isQuery() {
        return p.isQuery();
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return p.getParameters();
    }

    @Override
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        return p.query(maxRows);
    }

    @Override
    public int executeUpdate() {
        return p.update();
    }

    @Override
    public void close() {
        p = null;
    }

    @Override
    public void cancel() {
        p.checkCanceled();
    }

    @Override
    public ResultInterface getMetaData() {
        return p.queryMeta();
    }
}