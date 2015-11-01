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
package org.lealone.cluster.streaming.messages;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.cluster.db.TypeSizes;
import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.io.DataOutputPlus;
import org.lealone.cluster.io.IVersionedSerializer;

/**
 * StreamingFileHeader is appended before sending actual data to describe what it's sending.
 */
public class FileMessageHeader {
    public static IVersionedSerializer<FileMessageHeader> serializer = new FileMessageHeaderSerializer();

    public final String mapName;
    public final int sequenceNumber;
    public final long estimatedKeys;
    public final List<Range<Token>> ranges;

    public FileMessageHeader(String mapName, int sequenceNumber, long estimatedKeys, List<Range<Token>> ranges) {
        this.mapName = mapName;
        this.sequenceNumber = sequenceNumber;
        this.estimatedKeys = estimatedKeys;
        this.ranges = ranges;
    }

    /**
     * @return total file size to transfer in bytes
     */
    public long size() {
        // long size = 0;
        // for (Pair<Long, Long> section : sections)
        // size += section.right - section.left;
        // return size;
        return 0;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Header (");
        sb.append("mapName: ").append(mapName);
        sb.append(", #").append(sequenceNumber);
        sb.append(", estimated keys: ").append(estimatedKeys);
        // sb.append(", transfer size: ").append(size());
        sb.append(')');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        FileMessageHeader that = (FileMessageHeader) o;
        return sequenceNumber == that.sequenceNumber && mapName.equals(that.mapName);
    }

    @Override
    public int hashCode() {
        int result = mapName.hashCode();
        result = 31 * result + sequenceNumber;
        return result;
    }

    static class FileMessageHeaderSerializer implements IVersionedSerializer<FileMessageHeader> {
        @Override
        public void serialize(FileMessageHeader header, DataOutputPlus out, int version) throws IOException {
            out.writeUTF(header.mapName);
            out.writeInt(header.sequenceNumber);

            out.writeLong(header.estimatedKeys);
            out.writeInt(header.ranges.size());
            for (Range<Token> range : header.ranges) {
                Token.serializer.serialize(range.left, out);
                Token.serializer.serialize(range.right, out);
            }
        }

        @Override
        public FileMessageHeader deserialize(DataInput in, int version) throws IOException {
            String mapName = in.readUTF();
            int sequenceNumber = in.readInt();
            long estimatedKeys = in.readLong();
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++) {
                Token left = Token.serializer.deserialize(in);
                Token right = Token.serializer.deserialize(in);
                ranges.add(new Range<>(left, right));
            }

            return new FileMessageHeader(mapName, sequenceNumber, estimatedKeys, ranges);
        }

        @Override
        public long serializedSize(FileMessageHeader header, int version) {
            long size = TypeSizes.NATIVE.sizeof(header.mapName);
            size += TypeSizes.NATIVE.sizeof(header.sequenceNumber);

            size += TypeSizes.NATIVE.sizeof(header.estimatedKeys);

            size += TypeSizes.NATIVE.sizeof(header.ranges.size());
            for (Range<Token> range : header.ranges) {
                size += Token.serializer.serializedSize(range.left, TypeSizes.NATIVE);
                size += Token.serializer.serializedSize(range.right, TypeSizes.NATIVE);
            }

            return size;
        }
    }
}
