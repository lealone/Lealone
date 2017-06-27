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
package org.lealone.mvcc.log;

import java.nio.ByteBuffer;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.ValueString;
import org.lealone.storage.type.StorageDataType;

public class RedoLogValueType implements StorageDataType {

    private static ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    @Override
    public int compare(Object a, Object b) {
        throw DbException.getUnsupportedException("compare");
    }

    @Override
    public int getMemory(Object obj) {
        throw DbException.getUnsupportedException("getMemory");
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        RedoLogValue v = (RedoLogValue) obj;

        if (v.checkpoint != null) {
            buff.put((byte) 0);
            buff.putVarLong(v.checkpoint);
        } else if (v.droppedMap != null) {
            buff.put((byte) 3);
            ValueString.type.write(buff, v.droppedMap);
        } else {
            if (v.transactionName == null) {
                buff.put((byte) 1);
            } else {
                buff.put((byte) 2);
                ValueString.type.write(buff, v.transactionName);
                ValueString.type.write(buff, v.allLocalTransactionNames);
                buff.putVarLong(v.commitTimestamp);
            }
            buff.putVarInt(v.values.remaining());
            buff.put(v.values);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        int type = buff.get();
        if (type == 0)
            return new RedoLogValue(DataUtils.readVarLong(buff));
        else if (type == 3) {
            String droppedMap = ValueString.type.read(buff);
            return new RedoLogValue(droppedMap);
        }

        RedoLogValue v = new RedoLogValue();
        if (type == 2) {
            v.transactionName = ValueString.type.read(buff);
            v.allLocalTransactionNames = ValueString.type.read(buff);
            v.commitTimestamp = DataUtils.readVarLong(buff);
        }

        int len = DataUtils.readVarInt(buff);
        if (len > 0) {
            byte[] value = new byte[len];
            buff.get(value);
            v.values = ByteBuffer.wrap(value);
        } else {
            v.values = EMPTY_BUFFER;
        }
        return v;
    }

}
