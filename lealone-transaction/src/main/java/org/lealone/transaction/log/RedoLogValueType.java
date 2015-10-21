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
package org.lealone.transaction.log;

import java.nio.ByteBuffer;

import org.lealone.common.message.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.storage.type.DataType;
import org.lealone.storage.type.StringDataType;
import org.lealone.storage.type.WriteBuffer;

public class RedoLogValueType implements DataType {

    @Override
    public int compare(Object a, Object b) {
        throw DbException.getUnsupportedException("compare");
    }

    @Override
    public int getMemory(Object obj) {
        throw DbException.getUnsupportedException("getMemory");
    }

    @Override
    public void write(WriteBuffer buff, Object obj) {
        RedoLogValue v = (RedoLogValue) obj;
        StringDataType.INSTANCE.write(buff, v.mapName);
        buff.putVarInt(v.key.remaining());
        buff.put(v.key);
        buff.putVarInt(v.value.remaining());
        buff.put(v.value);
    }

    @Override
    public void write(WriteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            write(buff, obj[i]);
        }
    }

    @Override
    public Object read(ByteBuffer buff) {
        String mapName = StringDataType.INSTANCE.read(buff);

        byte[] key = new byte[DataUtils.readVarInt(buff)];
        buff.get(key);
        ByteBuffer keyBuffer = ByteBuffer.wrap(key);

        ByteBuffer valueBuffer;
        int len = DataUtils.readVarInt(buff);
        if (len > 0) {
            byte[] value = new byte[len];
            buff.get(value);
            valueBuffer = ByteBuffer.wrap(value);
        } else {
            valueBuffer = LogChunkMap.EMPTY_BUFFER;
        }
        return new RedoLogValue(mapName, keyBuffer, valueBuffer);
    }

    @Override
    public void read(ByteBuffer buff, Object[] obj, int len, boolean key) {
        for (int i = 0; i < len; i++) {
            obj[i] = read(buff);
        }
    }

}
