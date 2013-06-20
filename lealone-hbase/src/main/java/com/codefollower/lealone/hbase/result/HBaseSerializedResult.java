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
package com.codefollower.lealone.hbase.result;

import java.util.List;

import com.codefollower.lealone.command.CommandInterface;
import com.codefollower.lealone.result.DelegatedResult;
import com.codefollower.lealone.result.ResultInterface;

public class HBaseSerializedResult extends DelegatedResult {
    private final static int UNKNOW_ROW_COUNT = -1;
    private final List<ResultInterface> results;
    private final List<? extends CommandInterface> commands;
    private final int maxRows;
    private final boolean scrollable;

    private final int size;
    private int index = 0;

    public HBaseSerializedResult(List<? extends CommandInterface> commands, int maxRows, boolean scrollable) {
        this.results = null;
        this.commands = commands;
        this.maxRows = maxRows;
        this.scrollable = scrollable;
        this.size = commands.size();
        nextResult();
    }

    public HBaseSerializedResult(List<ResultInterface> results) {
        this.results = results;
        this.commands = null;
        this.maxRows = -1;
        this.scrollable = false;
        this.size = results.size();
        nextResult();
    }

    private boolean nextResult() {
        if (index >= size)
            return false;

        if (result != null)
            result.close();

        if (results != null)
            result = results.get(index++);
        else
            result = commands.get(index++).executeQuery(maxRows, scrollable);
        return true;
    }

    @Override
    public boolean next() {
        boolean next = result.next();
        if (!next) {
            next = nextResult();
            if (next)
                next = result.next();
        }
        return next;
    }

    @Override
    public int getRowCount() {
        return UNKNOW_ROW_COUNT;
    }
}
