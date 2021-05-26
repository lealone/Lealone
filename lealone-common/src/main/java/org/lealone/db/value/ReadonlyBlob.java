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
package org.lealone.db.value;

import java.io.OutputStream;
import java.sql.SQLException;

import org.lealone.common.trace.Trace;

/**
 * Represents a readonly BLOB value.
 */
public class ReadonlyBlob extends BlobBase {

    public ReadonlyBlob(Value value) {
        this.value = value;
        this.trace = Trace.NO_TRACE;
    }

    public ReadonlyBlob(String value) {
        this(ValueString.get(value));
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw unsupported("LOB update");
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw unsupported("LOB update");
    }
}
