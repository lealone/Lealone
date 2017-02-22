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
package org.lealone.aose.server;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;

import org.lealone.aose.io.DataOutputPlus;
import org.lealone.aose.io.IVersionedSerializer;
import org.lealone.aose.util.TypeSizes;

public class PullSchemaAck {
    public final ArrayList<String> sqls;

    public PullSchemaAck(ArrayList<String> sqls) {
        this.sqls = sqls;
    }

    public static final IVersionedSerializer<PullSchemaAck> serializer = new PullSchemaAckerializer();

    private static class PullSchemaAckerializer implements IVersionedSerializer<PullSchemaAck> {
        @Override
        public void serialize(PullSchemaAck t, DataOutputPlus out, int version) throws IOException {
            out.writeInt(t.sqls.size());
            for (String sql : t.sqls)
                out.writeUTF(sql);
        }

        @Override
        public PullSchemaAck deserialize(DataInput in, int version) throws IOException {
            int size = in.readInt();
            ArrayList<String> sqls = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
                sqls.add(in.readUTF());
            return new PullSchemaAck(sqls);
        }

        @Override
        public long serializedSize(PullSchemaAck t, int version) {
            long size = TypeSizes.NATIVE.sizeof(t.sqls.size());
            for (String sql : t.sqls)
                size += TypeSizes.NATIVE.sizeof(sql);
            return size;
        }
    }

}
