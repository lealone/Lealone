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

import org.lealone.aose.io.DataOutputPlus;
import org.lealone.aose.io.IVersionedSerializer;

public class PullSchema {

    public static final IVersionedSerializer<PullSchema> serializer = new PullSchemaSerializer();

    private static class PullSchemaSerializer implements IVersionedSerializer<PullSchema> {
        @Override
        public void serialize(PullSchema t, DataOutputPlus out, int version) throws IOException {
        }

        @Override
        public PullSchema deserialize(DataInput in, int version) throws IOException {
            return new PullSchema();
        }

        @Override
        public long serializedSize(PullSchema t, int version) {
            return 0;
        }
    }

}
