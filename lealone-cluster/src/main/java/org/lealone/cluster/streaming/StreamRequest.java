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
package org.lealone.cluster.streaming;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.lealone.cluster.db.TypeSizes;
import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.io.DataOutputPlus;
import org.lealone.cluster.io.IVersionedSerializer;

public class StreamRequest {
    public static final IVersionedSerializer<StreamRequest> serializer = new StreamRequestSerializer();

    public final String dbName;
    public final Collection<Range<Token>> ranges;
    public final Collection<String> tableNames = new HashSet<>();

    public StreamRequest(String dbName, Collection<Range<Token>> ranges, Collection<String> tableNames) {
        this.dbName = dbName;
        this.ranges = ranges;
        this.tableNames.addAll(tableNames);
    }

    public static class StreamRequestSerializer implements IVersionedSerializer<StreamRequest> {
        @Override
        public void serialize(StreamRequest request, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(request.dbName);
            out.writeInt(request.ranges.size());
            for (Range<Token> range : request.ranges) {
                // MessagingService.validatePartitioner(range);
                Token.serializer.serialize(range.left, out);
                Token.serializer.serialize(range.right, out);
            }
            out.writeInt(request.tableNames.size());
            for (String cf : request.tableNames)
                out.writeUTF(cf);
        }

        @Override
        public StreamRequest deserialize(DataInput in, int version) throws IOException {
            String dbName = in.readUTF();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++) {
                Token left = Token.serializer.deserialize(in);
                Token right = Token.serializer.deserialize(in);
                ranges.add(new Range<>(left, right));
            }
            int cfCount = in.readInt();
            List<String> tableNames = new ArrayList<>(cfCount);
            for (int i = 0; i < cfCount; i++)
                tableNames.add(in.readUTF());
            return new StreamRequest(dbName, ranges, tableNames);
        }

        @Override
        public long serializedSize(StreamRequest request, int version) {
            long size = TypeSizes.NATIVE.sizeof(request.dbName);
            size += TypeSizes.NATIVE.sizeof(request.ranges.size());
            for (Range<Token> range : request.ranges) {
                size += Token.serializer.serializedSize(range.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize(range.right, TypeSizes.NATIVE);
            }
            size += TypeSizes.NATIVE.sizeof(request.tableNames.size());
            for (String cf : request.tableNames)
                size += TypeSizes.NATIVE.sizeof(cf);
            return size;
        }
    }
}
