/*
 * Copyright 1999-2012 Alibaba Group.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *      http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.plugins.mysql.server.protocol;

import java.math.BigDecimal;
import java.util.List;

import com.lealone.db.CommandParameter;
import com.lealone.db.session.ServerSession;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueByte;
import com.lealone.db.value.ValueBytes;
import com.lealone.db.value.ValueDate;
import com.lealone.db.value.ValueDecimal;
import com.lealone.db.value.ValueDouble;
import com.lealone.db.value.ValueFloat;
import com.lealone.db.value.ValueInt;
import com.lealone.db.value.ValueLong;
import com.lealone.db.value.ValueNull;
import com.lealone.db.value.ValueShort;
import com.lealone.db.value.ValueString;
import com.lealone.db.value.ValueTime;
import com.lealone.db.value.ValueTimestamp;
import com.lealone.sql.PreparedSQLStatement;

/**
 * <pre>
 * Bytes Name
 * ----- ----
 * 1 code
 * 4 statement_id
 * 1 flags
 * 4 iteration_count
 * (param_count+7)/8 null_bit_map
 * 1 new_parameter_bound_flag (if new_params_bound == 1:)
 * n*2 type of parameters
 * n values for the parameters
 * --------------------------------------------------------------------------------
 * code: always COM_EXECUTE
 *
 * statement_id: statement identifier
 *
 * flags: reserved for future use. In MySQL 4.0, always 0.
 * In MySQL 5.0:
 * 0: CURSOR_TYPE_NO_CURSOR
 * 1: CURSOR_TYPE_READ_ONLY
 * 2: CURSOR_TYPE_FOR_UPDATE
 * 4: CURSOR_TYPE_SCROLLABLE
 *
 * iteration_count: reserved for future use. Currently always 1.
 *
 * null_bit_map: A bitmap indicating parameters that are NULL.
 * Bits are counted from LSB, using as many bytes
 * as necessary ((param_count+7)/8)
 * i.e. if the first parameter (parameter 0) is NULL, then
 * the least significant bit in the first byte will be 1.
 *
 * new_parameter_bound_flag: Contains 1 if this is the first time
 * that "execute" has been called, or if
 * the parameters have been rebound.
 *
 * type: Occurs once for each parameter;
 * The highest significant bit of this 16-bit value
 * encodes the unsigned property. The other 15 bits
 * are reserved for the type (only 8 currently used).
 * This block is sent when parameters have been rebound
 * or when a prepared statement is executed for the
 * first time.
 *
 * values: for all non-NULL values, each parameters appends its value
 * as described in Row Data Packet: Binary (column values)
 * @see http://dev.mysql.com/doc/internals/en/execute-packet.html
 * </pre>
 *
 * @author xianmao.hexm 2012-8-28
 * @author zhh
 */
public class ExecutePacket extends RequestPacket {

    public byte code;
    public long statementId;
    public byte flags;
    public long iterationCount;
    public byte[] nullBitMap;
    public byte newParameterBoundFlag;
    public int[] types;

    @Override
    public String getPacketInfo() {
        return "MySQL Execute Packet";
    }

    public void read(PacketInput in, String charset, ServerSession session) {
        setHeader(in);
        code = in.read();
        statementId = in.readUB4();
        flags = in.read();
        iterationCount = in.readUB4();

        PreparedSQLStatement stmt = (PreparedSQLStatement) session.getCache((int) statementId);
        List<? extends CommandParameter> params = stmt.getParameters();
        int parameterCount = params.size();
        types = new int[parameterCount];

        // 读取NULL指示器数据
        nullBitMap = new byte[(parameterCount + 7) / 8];
        for (int i = 0; i < nullBitMap.length; i++) {
            nullBitMap[i] = in.read();
        }

        // 当newParameterBoundFlag==1时，更新参数类型。
        newParameterBoundFlag = in.read();
        if (newParameterBoundFlag == (byte) 1) {
            for (int i = 0; i < parameterCount; i++) {
                types[i] = in.readUB2();
            }
        }

        // 设置参数类型和读取参数值
        byte[] nullBitMap = this.nullBitMap;
        for (int i = 0; i < parameterCount; i++) {
            CommandParameter p = params.get(i);
            if ((nullBitMap[i / 8] & (1 << (i & 7))) != 0) {
                p.setValue(ValueNull.INSTANCE);
            } else {
                p.setValue(getValue(in, types[i], charset));
            }
        }
    }

    private static Value getValue(PacketInput in, int type, String charset) {
        switch (type & 0xff) {
        case Fields.FIELD_TYPE_BIT:
            return ValueBytes.get(in.readBytesWithLength());
        case Fields.FIELD_TYPE_TINY:
            return ValueByte.get(in.read());
        case Fields.FIELD_TYPE_SHORT:
            return ValueShort.get((short) in.readUB2());
        case Fields.FIELD_TYPE_LONG:
            return ValueInt.get(in.readInt());
        case Fields.FIELD_TYPE_LONGLONG:
            return ValueLong.get(in.readLong());
        case Fields.FIELD_TYPE_FLOAT:
            return ValueFloat.get(in.readFloat());
        case Fields.FIELD_TYPE_DOUBLE:
            return ValueDouble.get(in.readDouble());
        case Fields.FIELD_TYPE_TIME:
            return ValueTime.get(in.readTime());
        case Fields.FIELD_TYPE_DATE:
        case Fields.FIELD_TYPE_DATETIME:
        case Fields.FIELD_TYPE_TIMESTAMP: {
            Object value = in.readDate();
            if (value instanceof java.sql.Date) {
                return ValueDate.get((java.sql.Date) value);
            } else {
                return ValueTimestamp.get((java.sql.Timestamp) value);
            }
        }
        case Fields.FIELD_TYPE_VAR_STRING:
        case Fields.FIELD_TYPE_STRING:
        case Fields.FIELD_TYPE_VARCHAR: {
            String value = in.readStringWithLength(charset);
            if (value == null) {
                return ValueNull.INSTANCE;
            }
            return ValueString.get(value);
        }
        case Fields.FIELD_TYPE_DECIMAL:
        case Fields.FIELD_TYPE_NEW_DECIMAL: {
            BigDecimal value = in.readBigDecimal();
            if (value == null) {
                return ValueNull.INSTANCE;
            }
            return ValueDecimal.get(value);
        }
        default:
            throw new IllegalArgumentException("bindValue error, unsupported type:" + type);
        }
    }
}
