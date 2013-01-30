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

import java.util.List;
import com.codefollower.yourbase.command.CommandInterface;
import com.codefollower.yourbase.hbase.engine.HBaseSession;
import com.codefollower.yourbase.result.ResultInterface;
import com.codefollower.yourbase.value.Value;

public class HBaseSerializedResult implements ResultInterface {
    private final static int UNKNOW_ROW_COUNT = -1;
    //private final HBaseSession session;
    private final List<CommandInterface> commands;
    private final int maxRows;
    private final boolean scrollable;

    private final int size;
    private int index = 0;

    private ResultInterface result;
    //private boolean isLast = false;

    public HBaseSerializedResult(HBaseSession session, List<CommandInterface> commands, int maxRows, boolean scrollable) {
        //this.session = session;
        this.commands = commands;
        this.maxRows = maxRows;
        this.scrollable = scrollable;
        this.size = commands.size();
        nextResult();
    }

    private boolean nextResult() {
        if (index >= size)
            return false;

        if (result != null)
            result.close();

        result = commands.get(index++).executeQuery(maxRows, scrollable);
        //        if (index >= size)
        //            isLast = true;
        return true;
    }

    @Override
    public void reset() {
        result.reset();
    }

    @Override
    public Value[] currentRow() {
        return result.currentRow();
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
    public int getRowId() {
        return result.getRowId();
    }

    @Override
    public int getVisibleColumnCount() {
        return result.getVisibleColumnCount();
    }

    @Override
    public int getRowCount() {
        //return isLast ? result.getRowCount() : UNKNOW_ROW_COUNT;
        return UNKNOW_ROW_COUNT;
    }

    @Override
    public boolean needToClose() {
        return result.needToClose();
    }

    @Override
    public void close() {
        result.close();
    }

    @Override
    public String getAlias(int i) {
        return result.getAlias(i);
    }

    @Override
    public String getSchemaName(int i) {
        return result.getSchemaName(i);
    }

    @Override
    public String getTableName(int i) {
        return result.getTableName(i);
    }

    @Override
    public String getColumnName(int i) {
        return result.getColumnName(i);
    }

    @Override
    public int getColumnType(int i) {
        return result.getColumnType(i);
    }

    @Override
    public long getColumnPrecision(int i) {
        return result.getColumnPrecision(i);
    }

    @Override
    public int getColumnScale(int i) {
        return result.getColumnScale(i);
    }

    @Override
    public int getDisplaySize(int i) {
        return result.getDisplaySize(i);
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return result.isAutoIncrement(i);
    }

    @Override
    public int getNullable(int i) {
        return result.getNullable(i);
    }

    @Override
    public void setFetchSize(int fetchSize) {
        result.setFetchSize(fetchSize);
    }

    @Override
    public int getFetchSize() {
        return result.getFetchSize();
    }

}
