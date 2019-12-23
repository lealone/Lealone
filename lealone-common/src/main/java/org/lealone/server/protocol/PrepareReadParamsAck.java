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
package org.lealone.server.protocol;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.CommandParameter;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.value.Value;
import org.lealone.net.NetInputStream;
import org.lealone.net.NetOutputStream;

public class PrepareReadParamsAck implements AckPacket {

    public final boolean isQuery;
    public final List<? extends CommandParameter> params;

    public PrepareReadParamsAck(boolean isQuery, List<? extends CommandParameter> params) {
        this.isQuery = isQuery;
        this.params = params;
    }

    @Override
    public PacketType getType() {
        return PacketType.PREPARED_STATEMENT_PREPARE_ACK;
    }

    @Override
    public void encode(NetOutputStream out, int version) throws IOException {
        out.writeBoolean(isQuery);
        out.writeInt(params.size());
        for (CommandParameter p : params) {
            writeParameterMetaData(out, p);
        }
    }

    /**
     * Write the parameter meta data to the transfer object.
     *
     * @param p the parameter
     */
    private static void writeParameterMetaData(NetOutputStream out, CommandParameter p) throws IOException {
        out.writeInt(p.getType());
        out.writeLong(p.getPrecision());
        out.writeInt(p.getScale());
        out.writeInt(p.getNullable());
    }

    public static final Decoder decoder = new Decoder();

    private static class Decoder implements PacketDecoder<PrepareReadParamsAck> {
        @Override
        public PrepareReadParamsAck decode(NetInputStream in, int version) throws IOException {
            boolean isQuery = in.readBoolean();
            int paramCount = in.readInt();
            ArrayList<CommandParameter> params = new ArrayList<>(paramCount);
            for (int i = 0; i < paramCount; i++) {
                ClientCommandParameter p = new ClientCommandParameter(i);
                p.readMetaData(in);
                params.add(p);
            }
            return new PrepareReadParamsAck(isQuery, params);
        }
    }

    /**
     * A client side parameter.
     */
    private static class ClientCommandParameter implements CommandParameter {

        private final int index;
        private Value value;
        private int dataType = Value.UNKNOWN;
        private long precision;
        private int scale;
        private int nullable = ResultSetMetaData.columnNullableUnknown;

        public ClientCommandParameter(int index) {
            this.index = index;
        }

        @Override
        public int getIndex() {
            return index;
        }

        @Override
        public void setValue(Value newValue, boolean closeOld) {
            if (closeOld && value != null) {
                value.close();
            }
            value = newValue;
        }

        @Override
        public void setValue(Value value) {
            this.value = value;
        }

        @Override
        public Value getValue() {
            return value;
        }

        @Override
        public void checkSet() {
            if (value == null) {
                throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "#" + (index + 1));
            }
        }

        @Override
        public boolean isValueSet() {
            return value != null;
        }

        @Override
        public int getType() {
            return value == null ? dataType : value.getType();
        }

        @Override
        public long getPrecision() {
            return value == null ? precision : value.getPrecision();
        }

        @Override
        public int getScale() {
            return value == null ? scale : value.getScale();
        }

        @Override
        public int getNullable() {
            return nullable;
        }

        /**
         * Read the parameter meta data from the out object.
         *
         * @param in the NetInputStream
         */
        public void readMetaData(NetInputStream in) throws IOException {
            dataType = in.readInt();
            precision = in.readLong();
            scale = in.readInt();
            nullable = in.readInt();
        }
    }
}
