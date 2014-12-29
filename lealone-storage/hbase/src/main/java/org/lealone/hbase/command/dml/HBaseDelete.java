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

import org.lealone.command.dml.Delete;
import org.lealone.engine.Session;

public class HBaseDelete extends Delete implements UpdateOrDelete {
    private final UpdateOrDeleteSupport updateOrDeleteSupport;

    public HBaseDelete(Session session) {
        super(session);
        updateOrDeleteSupport = new UpdateOrDeleteSupport(session, this);
    }

    @Override
    public void prepare() {
        super.prepare();
        if (tableFilter.getTable().isDistributed())
            updateOrDeleteSupport.postPrepare(tableFilter);
        else
            setExecuteDirec(true);
    }

    @Override
    public int update() {
        if (isExecuteDirec())
            return super.update();
        else
            return updateOrDeleteSupport.update();
    }

    @Override
    public WhereClauseSupport getWhereClauseSupport() {
        return updateOrDeleteSupport.getWhereClauseSupport();
    }

    @Override
    public int internalUpdate() {
        return super.update();
    }

}
