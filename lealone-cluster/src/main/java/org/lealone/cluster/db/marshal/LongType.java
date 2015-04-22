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
package org.lealone.cluster.db.marshal;

import java.nio.ByteBuffer;

import org.lealone.cluster.utils.ByteBufferUtil;

public class LongType extends AbstractType<Long> {
    public static final LongType instance = new LongType();

    LongType() {
    } // singleton

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return compareLongs(o1, o2);
    }

    public static int compareLongs(ByteBuffer o1, ByteBuffer o2) {
        if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        int diff = o1.get(o1.position()) - o2.get(o2.position());
        if (diff != 0)
            return diff;

        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    @Override
    public ByteBuffer fromString(String source) throws MarshalException {
        // Return an empty ByteBuffer for an empty string.
        if (source.isEmpty())
            return ByteBufferUtil.EMPTY_BYTE_BUFFER;

        long longType;

        try {
            longType = Long.parseLong(source);
        } catch (Exception e) {
            throw new MarshalException(String.format("unable to make long from '%s'", source), e);
        }

        return decompose(longType);
    }

    @Override
    public TypeSerializer<Long> getSerializer() {
        return LongSerializer.instance;
    }

    private static class LongSerializer implements TypeSerializer<Long> {
        public static final LongSerializer instance = new LongSerializer();

        @Override
        public Long deserialize(ByteBuffer bytes) {
            return bytes.remaining() == 0 ? null : ByteBufferUtil.toLong(bytes);
        }

        @Override
        public ByteBuffer serialize(Long value) {
            return value == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : ByteBufferUtil.bytes(value);
        }

        @Override
        public void validate(ByteBuffer bytes) throws MarshalException {
            if (bytes.remaining() != 8 && bytes.remaining() != 0)
                throw new MarshalException(String.format("Expected 8 or 0 byte long (%d)", bytes.remaining()));
        }

        @Override
        public String toString(Long value) {
            return value == null ? "" : String.valueOf(value);
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    }
}
