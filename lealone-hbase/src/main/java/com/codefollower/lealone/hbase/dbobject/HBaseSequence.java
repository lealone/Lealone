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
package com.codefollower.lealone.hbase.dbobject;

import com.codefollower.lealone.command.CommandRemote;
import com.codefollower.lealone.dbobject.Schema;
import com.codefollower.lealone.dbobject.Sequence;
import com.codefollower.lealone.engine.Session;
import com.codefollower.lealone.engine.SessionRemote;
import com.codefollower.lealone.hbase.engine.HBaseSession;
import com.codefollower.lealone.hbase.engine.SessionRemotePool;
import com.codefollower.lealone.message.DbException;
import com.codefollower.lealone.result.ResultInterface;

public class HBaseSequence extends Sequence {

    public HBaseSequence(Schema schema, int id, String name, boolean belongsToTable) {
        super(schema, id, name, belongsToTable);
    }

    @Override
    public synchronized void flush(Session session) {
        HBaseSession s = (HBaseSession) session;
        if (s.getRegionServer() != null) {
            SessionRemote sr = null;
            CommandRemote cr = null;
            try {
                sr = SessionRemotePool.getMasterSessionRemote(s.getOriginalProperties());
                cr = SessionRemotePool.getCommandRemote(sr, "ALTER SEQUENCE " + getSQL() + " NEXT VALUE MARGIN", null, 1);
                //cr.executeUpdate();
                ResultInterface ri = cr.executeQuery(-1, false);
                ri.next();
                valueWithMargin = ri.currentRow()[0].getLong();
                value = valueWithMargin - increment * cacheSize;
            } catch (Exception e) {
                throw DbException.convert(e);
            } finally {
                SessionRemotePool.release(sr);
                if (cr != null)
                    cr.close();
            }
        } else if (s.getMaster() != null) {
            super.flush(session);
        }
    }

    public synchronized long alterNextValueMargin(Session session) {
        long value = this.value;
        long valueWithMargin = increment * cacheSize;
        value = value + valueWithMargin;
        setStartValue(value);

        flush(session);

        return value;
    }
}
