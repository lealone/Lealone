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
import org.lealone.cluster.utils.Hex;

public class BytesType extends AbstractType<ByteBuffer> {
    public static final BytesType instance = new BytesType();

    BytesType() {
    } // singleton

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    @Override
    public ByteBuffer fromString(String source) {
        try {
            return ByteBuffer.wrap(Hex.hexToBytes(source));
        } catch (NumberFormatException e) {
            throw new MarshalException(String.format("cannot parse '%s' as hex bytes", source), e);
        }
    }

    @Override
    public boolean isValueCompatibleWithInternal(AbstractType<?> otherType) {
        // BytesType can read anything
        return true;
    }

    @Override
    public boolean isByteOrderComparable() {
        return true;
    }

    @Override
    public TypeSerializer<ByteBuffer> getSerializer() {
        return BytesSerializer.instance;
    }

    private static class BytesSerializer implements TypeSerializer<ByteBuffer> {
        public static final BytesSerializer instance = new BytesSerializer();

        @Override
        public ByteBuffer serialize(ByteBuffer bytes) {
            // We make a copy in case the user modifies the input
            return bytes.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer value) {
            // This is from the DB, so it is not shared with someone else
            return value;
        }

        @Override
        public void validate(ByteBuffer bytes) throws MarshalException {
            // all bytes are legal.
        }

        @Override
        public String toString(ByteBuffer value) {
            return ByteBufferUtil.bytesToHex(value);
        }

        @Override
        public Class<ByteBuffer> getType() {
            return ByteBuffer.class;
        }
    }
}
