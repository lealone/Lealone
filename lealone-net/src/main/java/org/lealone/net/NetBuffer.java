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
package org.lealone.net;

public interface NetBuffer {

    NetBuffer appendBuffer(NetBuffer buff);

    int length();

    NetBuffer slice(int start, int end);

    NetBuffer getBuffer(int start, int end);

    short getUnsignedByte(int pos);

    NetBuffer appendByte(byte b);

    NetBuffer appendBytes(byte[] bytes, int offset, int len);

    NetBuffer appendInt(int i);

    NetBuffer setByte(int pos, byte b);

    NetBuffer flip();

    default void recycle() {
    }
}
