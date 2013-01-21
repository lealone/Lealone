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
package com.codefollower.yourbase.hbase.engine;

import java.util.Properties;

import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;

import com.codefollower.yourbase.command.Command;
import com.codefollower.yourbase.command.Parser;
import com.codefollower.yourbase.command.dml.Query;
import com.codefollower.yourbase.constant.ErrorCode;
import com.codefollower.yourbase.dbobject.User;
import com.codefollower.yourbase.engine.Database;
import com.codefollower.yourbase.engine.Session;
import com.codefollower.yourbase.hbase.command.CommandProxy;
import com.codefollower.yourbase.hbase.command.HBaseParser;
import com.codefollower.yourbase.hbase.result.HBaseCombinedResult;
import com.codefollower.yourbase.message.DbException;
import com.codefollower.yourbase.result.CombinedResult;
import com.codefollower.yourbase.util.SmallLRUCache;

public class HBaseSession extends Session {

    private HMaster master;
    private HRegionServer regionServer;
    private byte[] regionName;
    private Properties originalProperties;

    public Properties getOriginalProperties() {
        return originalProperties;
    }

    public void setOriginalProperties(Properties originalProperties) {
        this.originalProperties = originalProperties;
    }

    public byte[] getRegionName() {
        return regionName;
    }

    public void setRegionName(byte[] regionName) {
        this.regionName = regionName;
    }

    public HMaster getMaster() {
        return master;
    }

    public void setMaster(HMaster master) {
        this.master = master;
    }

    public HRegionServer getRegionServer() {
        return regionServer;
    }

    public void setRegionServer(HRegionServer regionServer) {
        this.regionServer = regionServer;
    }

    public HBaseSession(Database database, User user, int id) {
        super(database, user, id);
    }

    protected Command prepareCommandInternal(String sql, boolean useProxy) {
        if (closed) {
            throw DbException.get(ErrorCode.CONNECTION_BROKEN_1, "session closed");
        }
        Command command;
        if (queryCacheSize > 0) {
            if (queryCache == null) {
                queryCache = SmallLRUCache.newInstance(queryCacheSize);
            } else {
                command = queryCache.get(sql);
                if (command != null && command.canReuse()) {
                    command.reuse();
                    return command;
                }
            }
        }
        Parser parser = createParser();
        command = parser.prepareCommand(sql);
        if (useProxy)
            command = new CommandProxy(parser, sql, command);
        if (queryCache != null) {
            if (command.isCacheable()) {
                queryCache.put(sql, command);
            }
        }
        return command;
    }

    public CombinedResult createCombinedResult(Query query, int maxrows) {
        return new HBaseCombinedResult(this, query, maxrows);
    }

    public HBaseDatabase getDatabase() {
        return (HBaseDatabase) database;
    }

    public Parser createParser() {
        return new HBaseParser(this);
    }
}
