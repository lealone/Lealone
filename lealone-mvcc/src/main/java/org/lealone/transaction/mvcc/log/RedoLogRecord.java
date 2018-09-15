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
package org.lealone.transaction.mvcc.log;

import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;

//RedoLog文件中会有三种类型的日志记录
public class RedoLogRecord {

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    // 1. 本地事务只包含这个字段
    public ByteBuffer values;

    // 2. 分布式事务多加这三个字段
    public String transactionName;
    public String allLocalTransactionNames;
    public long commitTimestamp;

    // 3. 检查点只有这个字段
    public boolean checkpoint;

    // 4. 已经被删除的map
    public String droppedMap;

    volatile boolean synced;

    public Long transactionId;

    public RedoLogRecord() {
    }

    public RedoLogRecord(Long transactionId, ByteBuffer values) {
        this.transactionId = transactionId;
        this.values = values;
    }

    public RedoLogRecord(boolean checkpoint) {
        this.checkpoint = checkpoint;
    }

    public RedoLogRecord(String droppedMap) {
        this.droppedMap = droppedMap;
    }

    void write(DataBuffer buff) {
        if (checkpoint) {
            buff.put((byte) 0);
        } else if (droppedMap != null) {
            buff.put((byte) 3);
            ValueString.type.write(buff, droppedMap);
        } else {
            if (transactionName == null) {
                buff.put((byte) 1);
            } else {
                buff.put((byte) 2);
                ValueString.type.write(buff, transactionName);
                ValueString.type.write(buff, allLocalTransactionNames);
                buff.putVarLong(commitTimestamp);
            }
            buff.putVarInt(values.remaining());
            buff.put(values);

            if (transactionId == null)
                buff.putVarLong(-1);
            else
                buff.putVarLong(transactionId);
        }
    }

    static RedoLogRecord read(ByteBuffer buff) {
        int type = buff.get();
        if (type == 0)
            return new RedoLogRecord(true);
        else if (type == 3) {
            String droppedMap = ValueString.type.read(buff);
            return new RedoLogRecord(droppedMap);
        }

        RedoLogRecord r = new RedoLogRecord();
        if (type == 2) {
            r.transactionName = ValueString.type.read(buff);
            r.allLocalTransactionNames = ValueString.type.read(buff);
            r.commitTimestamp = DataUtils.readVarLong(buff);
        }

        int len = DataUtils.readVarInt(buff);
        if (len > 0) {
            byte[] value = new byte[len];
            buff.get(value);
            r.values = ByteBuffer.wrap(value);
        } else {
            r.values = EMPTY_BUFFER;
        }

        long transactionId = DataUtils.readVarLong(buff);
        if (transactionId != -1)
            r.transactionId = transactionId;
        return r;
    }
}
