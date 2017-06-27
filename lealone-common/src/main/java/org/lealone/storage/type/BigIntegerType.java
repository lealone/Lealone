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
package org.lealone.storage.type;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;

public class BigIntegerType extends StorageDataTypeBase {

    @Override
    public int getType() {
        return TYPE_BIG_INTEGER;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        BigInteger a = (BigInteger) aObj;
        BigInteger b = (BigInteger) bObj;
        return a.compareTo(b);
    }

    @Override
    public int getMemory(Object obj) {
        return 100;
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        BigInteger x = (BigInteger) obj;
        if (BigInteger.ZERO.equals(x)) {
            buff.put((byte) TAG_BIG_INTEGER_0);
        } else if (BigInteger.ONE.equals(x)) {
            buff.put((byte) TAG_BIG_INTEGER_1);
        } else {
            int bits = x.bitLength();
            if (bits <= 63) {
                buff.put((byte) TAG_BIG_INTEGER_SMALL).putVarLong(x.longValue());
            } else {
                byte[] bytes = x.toByteArray();
                buff.put((byte) TYPE_BIG_INTEGER).putVarInt(bytes.length).put(bytes);
            }
        }
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        switch (tag) {
        case TAG_BIG_INTEGER_0:
            return BigInteger.ZERO;
        case TAG_BIG_INTEGER_1:
            return BigInteger.ONE;
        case TAG_BIG_INTEGER_SMALL:
            return BigInteger.valueOf(DataUtils.readVarLong(buff));
        }
        int len = DataUtils.readVarInt(buff);
        byte[] bytes = DataUtils.newBytes(len);
        buff.get(bytes);
        return new BigInteger(bytes);
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

}
