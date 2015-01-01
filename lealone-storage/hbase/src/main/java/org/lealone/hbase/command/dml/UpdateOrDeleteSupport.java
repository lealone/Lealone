/*
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
package org.lealone.hbase.command.dml;

import java.util.concurrent.Callable;

import org.lealone.command.Prepared;
import org.lealone.command.dml.Delete;
import org.lealone.command.dml.Update;
import org.lealone.hbase.command.CommandParallel;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;

public class UpdateOrDeleteSupport implements Callable<Integer> {
    private final WhereClauseSupport whereClauseSupport;
    private final HBaseSession session;
    private final Prepared prepared;
    private final Callable<Integer> callable;

    private SQLRoutingInfo sqlRoutingInfo;

    public UpdateOrDeleteSupport(Delete delete) {
        this.session = (HBaseSession) delete.getSession();
        this.prepared = delete;
        this.callable = delete;

        delete.getTableFilter().setPrepared(delete);
        whereClauseSupport = ((WithWhereClause) delete).getWhereClauseSupport();
        whereClauseSupport.setTableFilter(delete.getTableFilter());
    }

    public UpdateOrDeleteSupport(Update update) {
        this.session = (HBaseSession) update.getSession();
        this.prepared = update;
        this.callable = update;

        update.getTableFilter().setPrepared(update);
        whereClauseSupport = ((WithWhereClause) update).getWhereClauseSupport();
        whereClauseSupport.setTableFilter(update.getTableFilter());
    }

    public int execute() {
        try {
            int updateCount = 0;
            if (whereClauseSupport.getLocalRegionNames() != null) {
                updateCount = call().intValue();
            } else {
                sqlRoutingInfo = HBaseUtils.getSQLRoutingInfo(session, whereClauseSupport, prepared);

                if (sqlRoutingInfo.localRegion != null) {
                    whereClauseSupport.setCurrentRegionName(sqlRoutingInfo.localRegion);
                    updateCount = callable.call();
                } else if (sqlRoutingInfo.remoteCommand != null) {
                    updateCount = sqlRoutingInfo.remoteCommand.executeUpdate();
                } else {
                    if (sqlRoutingInfo.remoteCommands == null)
                        updateCount = call().intValue();
                    else
                        updateCount = CommandParallel.executeUpdate(sqlRoutingInfo, this);
                }
            }
            return updateCount;
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Integer call() throws Exception {
        int updateCount = 0;

        if (whereClauseSupport.getLocalRegionNames() != null) {
            for (String regionName : whereClauseSupport.getLocalRegionNames()) {
                whereClauseSupport.setCurrentRegionName(regionName);
                updateCount += callable.call();
            }
        } else if (sqlRoutingInfo.localRegions != null && !sqlRoutingInfo.localRegions.isEmpty()) {
            for (String regionName : sqlRoutingInfo.localRegions) {
                whereClauseSupport.setCurrentRegionName(regionName);
                updateCount += callable.call();
            }
        }
        return Integer.valueOf(updateCount);
    }
}
