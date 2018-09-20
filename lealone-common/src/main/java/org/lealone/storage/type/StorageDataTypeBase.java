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

import org.lealone.common.util.DataUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.value.Value;

public abstract class StorageDataTypeBase implements StorageDataType {

    public abstract int getType();

    public abstract void writeValue(DataBuffer buff, Value v);

    @Override
    public Object read(ByteBuffer buff) {
        int tag = buff.get();
        return readValue(buff, tag).getObject();
    }

    public Object read(ByteBuffer buff, int tag) {
        return readValue(buff, tag).getObject();
    }

    public Value readValue(ByteBuffer buff) {
        throw newInternalError();
    }

    public Value readValue(ByteBuffer buff, int tag) {
        return readValue(buff);
    }

    protected IllegalStateException newInternalError() {
        return DataUtils.newIllegalStateException(DataUtils.ERROR_INTERNAL, "Internal error");
    }

}
