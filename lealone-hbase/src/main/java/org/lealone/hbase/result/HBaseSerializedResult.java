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
package org.lealone.hbase.result;

import java.util.List;

import org.lealone.command.CommandInterface;
import org.lealone.command.dml.Select;
import org.lealone.result.DelegatedResult;
import org.lealone.result.ResultInterface;

public class HBaseSerializedResult extends DelegatedResult {
    private final static int UNKNOW_ROW_COUNT = -1;
    private final List<ResultInterface> results;
    private final List<? extends CommandInterface> commands;
    private final int maxRows;
    private final int limitRows;
    private final boolean scrollable;

    private final int size;
    private int index = 0;
    private int count = 0;

    public HBaseSerializedResult(List<? extends CommandInterface> commands, int maxRows, boolean scrollable, Select select) {
        this.results = null;
        this.commands = commands;
        this.maxRows = maxRows;
        this.limitRows = select.getLimitRows();
        this.scrollable = scrollable;
        this.size = commands.size();
        nextResult();
    }

    public HBaseSerializedResult(List<ResultInterface> results, Select select) {
        this.results = results;
        this.commands = null;
        this.maxRows = -1;
        this.limitRows = select.getLimitRows();
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
        count++;
        if (limitRows >= 0 && count > limitRows)
            return false;
        boolean next = result.next();
        if (!next) {
            boolean nextResult;
            while (true) {
                nextResult = nextResult();
                if (nextResult) {
                    next = result.next();
                    if (next)
                        return true;
                } else
                    return false;
            }
        }

        return next;
    }

    @Override
    public int getRowCount() {
        return UNKNOW_ROW_COUNT;
    }
}
