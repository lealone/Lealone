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

import java.util.Arrays;
import com.codefollower.lealone.command.dml.Select;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.CommandParallel;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;

public class HBaseSelect extends Select implements WithWhereClause {
    private final WhereClauseSupport whereClauseSupport = new WhereClauseSupport();
    private Task task;

    public HBaseSelect(Session session) {
        super(session);
    }

    @Override
    public void prepare() {
        super.prepare();

        whereClauseSupport.setTableFilter(topTableFilter);
    }

    @Override
    public ResultInterface query(int maxrows) {
        if (isExecuteDirec()) {
            return super.query(maxrows);
        }
        if (getLocalRegionNames() != null) {
            task = new Task();
            task.localRegions = Arrays.asList(getLocalRegionNames());
            return CommandParallel.executeQuery((HBaseSession) session, task, this, maxrows, false);
        } else {
            try {
                task = HBaseUtils.parseRowKey((HBaseSession) session, whereClauseSupport, this);
            } catch (Exception e) {
                throw DbException.convert(e);
            }

            if (task.localRegion != null) {
                whereClauseSupport.setRegionName(task.localRegion);
                return super.query(maxrows);
            } else if (task.remoteCommand != null) {
                return task.remoteCommand.executeQuery(maxrows, false);
            } else {
                return CommandParallel.executeQuery((HBaseSession) session, task, this, maxrows, false);
            }
        }
    }

    @Override
    public WhereClauseSupport getWhereClauseSupport() {
        return whereClauseSupport;
    }

}
