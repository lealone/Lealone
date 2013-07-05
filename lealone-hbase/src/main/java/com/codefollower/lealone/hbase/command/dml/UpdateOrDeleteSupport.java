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

import java.util.concurrent.Callable;

import com.codefollower.lealone.command.Prepared;
import com.codefollower.lealone.dbobject.table.TableFilter;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.hbase.command.CommandParallel;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.util.HBaseUtils;
import com.codefollower.lealone.message.DbException;

public class UpdateOrDeleteSupport implements Callable<Integer> {
    private final WhereClauseSupport whereClauseSupport = new WhereClauseSupport();
    private final HBaseSession session;
    private final UpdateOrDelete uod;
    private final Prepared prepared;

    private boolean isBatch = false;
    private Task task;

    public UpdateOrDeleteSupport(Session session, UpdateOrDelete uod) {
        this.session = (HBaseSession) session;
        this.uod = uod;
        this.prepared = (Prepared) uod;
    }

    public void postPrepare(TableFilter tableFilter) {
        if (session.getAutoCommit()) {
            session.setAutoCommit(false);
            isBatch = true;
        }
        tableFilter.setPrepared(prepared);
        whereClauseSupport.setTableFilter(tableFilter);
    }

    public int update() {
        try {
            int updateCount = 0;
            if (prepared.getLocalRegionNames() != null) {
                updateCount = call().intValue();
            } else {
                task = HBaseUtils.parseRowKey(session, whereClauseSupport, prepared);

                if (task.localRegion != null) {
                    whereClauseSupport.setRegionName(task.localRegion);
                    updateCount = uod.internalUpdate();
                } else if (task.remoteCommand != null) {
                    updateCount = task.remoteCommand.executeUpdate();
                } else {
                    if (task.remoteCommands == null)
                        updateCount = call().intValue();
                    else
                        updateCount = CommandParallel.executeUpdate(task, this);
                }
            }

            if (isBatch)
                session.commit(false);
            return updateCount;
        } catch (Exception e) {
            if (isBatch)
                session.rollback();
            throw DbException.convert(e);
        } finally {
            if (isBatch)
                session.setAutoCommit(true);
        }
    }

    @Override
    public Integer call() throws Exception {
        int updateCount = 0;

        if (prepared.getLocalRegionNames() != null) {
            for (String regionName : prepared.getLocalRegionNames()) {
                whereClauseSupport.setRegionName(regionName);
                updateCount += uod.internalUpdate();
            }
        } else if (task.localRegions != null && !task.localRegions.isEmpty()) {
            for (String regionName : task.localRegions) {
                whereClauseSupport.setRegionName(regionName);
                updateCount += uod.internalUpdate();
            }
        }
        return Integer.valueOf(updateCount);
    }

    public WhereClauseSupport getWhereClauseSupport() {
        return whereClauseSupport;
    }
}
