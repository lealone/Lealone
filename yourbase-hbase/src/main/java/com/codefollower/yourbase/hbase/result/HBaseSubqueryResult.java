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
package com.codefollower.yourbase.hbase.result;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.dml.Query;
import com.codefollower.yourbase.dbobject.table.TableFilter;
import com.codefollower.yourbase.hbase.command.CommandProxy;
import com.codefollower.yourbase.hbase.command.CommandSubquery;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.result.SubqueryResult;

public class HBaseSubqueryResult extends SubqueryResult {
    private CommandProxy commandProxy;

    public HBaseSubqueryResult(HBaseSession session, Query query, int maxrows) {
        Command c = query.getCommand();
        if (c == null)
            c = new CommandSubquery(session, query);

        commandProxy = new CommandProxy(session, query.getSQL(), c);
        commandProxy.setFetchSize(c.getFetchSize());
        result = commandProxy.executeQuery(maxrows, false);
    }

    public HBaseSubqueryResult(TableFilter filter) {
        this((HBaseSession) filter.getSession(), filter.getSelect(), -1);
    }

    @Override
    public void close() {
        commandProxy.close();
        super.close();
    }
}
