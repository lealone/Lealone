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

import java.nio.ByteBuffer;

import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;

public class CharacterType extends StorageDataTypeBase {

    @Override
    public int getType() {
        return TYPE_CHAR;
    }

    @Override
    public int compare(Object aObj, Object bObj) {
        Character a = (Character) aObj;
        Character b = (Character) bObj;
        return a.compareTo(b);
    }

    @Override
    public int getMemory(Object obj) {
        return 24;
    }

    @Override
    public void write(DataBuffer buff, Object obj) {
        buff.put((byte) TYPE_CHAR);
        buff.putChar(((Character) obj).charValue());
    }

    @Override
    public Object read(ByteBuffer buff, int tag) {
        return Character.valueOf(buff.getChar());
    }

    @Override
    public void writeValue(DataBuffer buff, Value v) {
        throw newInternalError();
    }

}
