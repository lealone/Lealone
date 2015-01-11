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
package org.lealone.hbase.command.ddl;

import java.io.IOException;

import org.lealone.command.FrontendCommand;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.ddl.DefineCommandWrapper;
import org.lealone.command.router.FrontendSessionPool;
import org.lealone.engine.FrontendSession;
import org.lealone.engine.Session;
import org.lealone.hbase.engine.HBaseSession;
import org.lealone.hbase.util.HBaseUtils;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;

//只重写了update、query两个方法
public class HBaseDefineCommandWrapper extends DefineCommandWrapper {
    private final HBaseSession session;
    private final String sql;

    public HBaseDefineCommandWrapper(Session session, DefineCommand dc, String sql) {
        super(session, dc);
        this.session = (HBaseSession) session;
        this.sql = sql;
    }

    @Override
    public int update() {
        if (session.isMaster()) {
            int updateCount = dc.update();
            session.getDatabase().addDDLRedoRecord(session, sql);
            return updateCount;
        } else if (dc.isLocal()) {
            return dc.update();
        } else {
            FrontendSession fs = null;
            FrontendCommand fc = null;
            try {
                fs = FrontendSessionPool.getFrontendSession(session.getOriginalProperties(),
                        HBaseUtils.getMasterURL(session.getDatabase().getShortName()));
                fc = FrontendSessionPool.getFrontendCommand(fs, sql, getParameters(), dc.getFetchSize());
                int updateCount = fc.executeUpdate();
                refreshMetaTable();
                return updateCount;
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                FrontendSessionPool.release(fs);
                if (fc != null)
                    fc.close();
            }
        }
    }

    @Override
    public ResultInterface query(int maxRows) {
        if (session.isMaster()) {
            return dc.query(maxRows);
        } else {
            FrontendSession fs = null;
            FrontendCommand fc = null;
            try {
                fs = FrontendSessionPool.getFrontendSession(session.getOriginalProperties(),
                        HBaseUtils.getMasterURL(session.getDatabase().getShortName()));
                fc = FrontendSessionPool.getFrontendCommand(fs, sql, getParameters(), dc.getFetchSize());
                ResultInterface ri = fc.executeQuery(maxRows, false);
                refreshMetaTable();
                return ri;
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                FrontendSessionPool.release(fs);
                if (fc != null)
                    fc.close();
            }
        }
    }

    private void refreshMetaTable() {
        session.getDatabase().refreshDDLRedoTable();
        try {
            HBaseUtils.reset(); //执行完DDL后，元数据已变动，清除HConnection中的相关缓存
        } catch (IOException e) {
            throw DbException.convert(e);
        }
    }
}
