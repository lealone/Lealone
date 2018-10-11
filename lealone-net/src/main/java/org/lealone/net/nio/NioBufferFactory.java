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
package org.lealone.net.nio;

import org.lealone.db.DataBuffer;
import org.lealone.net.NetBufferFactory;

public class NioBufferFactory implements NetBufferFactory {

    private static final NioBufferFactory instance = new NioBufferFactory();

    public static NioBufferFactory getInstance() {
        return instance;
    }

    private NioBufferFactory() {
    }

    @Override
    public NioBuffer createBuffer(int initialSizeHint) {
        return new NioBuffer(DataBuffer.create(initialSizeHint));
    }

}
