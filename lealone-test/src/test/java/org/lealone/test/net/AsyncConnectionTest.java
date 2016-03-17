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
package org.lealone.test.net;

import io.vertx.core.buffer.Buffer;

import org.lealone.net.AsyncConnection;

public class AsyncConnectionTest {

    public static void main(String[] args) {
        AsyncConnection c = new AsyncConnection(null);
        Buffer b = Buffer.buffer();
        b.appendInt(8); // packetLength
        b.appendInt(1);
        b.appendInt(2);
        c.handle(b);

        b = Buffer.buffer();
        b.appendInt(8); // packetLength
        b.appendInt(1);
        c.handle(b);

        b = Buffer.buffer();
        b.appendInt(2);
        c.handle(b);

        // c.tmpBuffer = null;
        b = Buffer.buffer();
        b.appendShort((short) 0);
        c.handle(b);
        b = Buffer.buffer();
        b.appendShort((short) 8);
        c.handle(b);

        // c.tmpBuffer = null;
        b = Buffer.buffer();
        // packet 1
        b.appendInt(8); // packetLength
        b.appendInt(1);
        b.appendInt(2);
        // packet 2
        b.appendInt(12); // packetLength
        b.appendInt(1);
        b.appendInt(2);
        b.appendInt(3);

        // packet 3 half
        b.appendInt(12); // packetLength
        b.appendInt(1);
        b.appendInt(2);
        c.handle(b);
        // packet 3
        b = Buffer.buffer();
        b.appendInt(3);
        c.handle(b);
    }

}
