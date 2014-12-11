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

import org.lealone.cluster.serializers.TypeSerializer;
import org.lealone.cluster.serializers.UTF8Serializer;
import org.lealone.cluster.utils.ByteBufferUtil;

public class UTF8Type extends AbstractType<String> {
    public static final UTF8Type instance = new UTF8Type();

    UTF8Type() {
    } // singleton

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2) {
        return ByteBufferUtil.compareUnsigned(o1, o2);
    }

    @Override
    public ByteBuffer fromString(String source) {
        return decompose(source);
    }

    //    @Override
    //    public boolean isCompatibleWith(AbstractType<?> previous)
    //    {
    //        // Anything that is ascii is also utf8, and they both use bytes
    //        // comparison
    //        return this == previous || previous == AsciiType.instance;
    //    }

    @Override
    public boolean isByteOrderComparable() {
        return true;
    }

    //    public CQL3Type asCQL3Type() {
    //        return CQL3Type.Native.TEXT;
    //    }

    @Override
    public TypeSerializer<String> getSerializer() {
        return UTF8Serializer.instance;
    }
}
