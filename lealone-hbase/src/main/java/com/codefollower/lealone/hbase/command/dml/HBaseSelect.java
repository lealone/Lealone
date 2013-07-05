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
import com.codefollower.lealone.result.ResultTarget;

public class HBaseSelect extends Select implements WithWhereClause {
    private final WhereClauseSupport whereClauseSupport = new WhereClauseSupport();
    private SQLRoutingInfo sqlRoutingInfo;

    public HBaseSelect(Session session) {
        super(session);
    }

    @Override
    public void prepare() {
        super.prepare();
        if (topTableFilter.getTable().isDistributed())
            whereClauseSupport.setTableFilter(topTableFilter);
        else
            setExecuteDirec(true);
    }

    @Override
    public ResultInterface query(int limit, ResultTarget target) {
        boolean addRowToResultTarget = true;
        ResultInterface result;

        if (isExecuteDirec()) {
            result = super.query(limit, target);
            addRowToResultTarget = false;
        } else if (getLocalRegionNames() != null) {
            sqlRoutingInfo = new SQLRoutingInfo();
            sqlRoutingInfo.localRegions = Arrays.asList(getLocalRegionNames());
            result = CommandParallel.executeQuery(session, sqlRoutingInfo, this, limit, false);
        } else {
            try {
                sqlRoutingInfo = HBaseUtils.getSQLRoutingInfo((HBaseSession) session, whereClauseSupport, this);
            } catch (Exception e) {
                throw DbException.convert(e);
            }

            if (sqlRoutingInfo.localRegion != null) {
                whereClauseSupport.setRegionName(sqlRoutingInfo.localRegion);
                result = super.query(limit, target);
                addRowToResultTarget = false;
            } else if (sqlRoutingInfo.remoteCommand != null) {
                sqlRoutingInfo.remoteCommand.setFetchSize(getFetchSize());
                result = sqlRoutingInfo.remoteCommand.executeQuery(limit, false);
            } else {
                result = CommandParallel.executeQuery(session, sqlRoutingInfo, this, limit, false);
            }
        }

        if (addRowToResultTarget && target != null) {
            while (result.next()) {
                target.addRow(result.currentRow());
            }
            result.reset();
        }
        return result;
    }

    @Override
    public WhereClauseSupport getWhereClauseSupport() {
        return whereClauseSupport;
    }

}
