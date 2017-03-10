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
package org.lealone.replication;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.lealone.db.Session;
import org.lealone.storage.type.DataType;

public interface Replication {

    List<InetAddress> getReplicationEndpoints(Object key);

    InetAddress getLocalEndpoint();

    Object put(Object key, Object value, DataType valueType, Session session);

    Object get(Object key, Session session);

    void addLeafPage(ByteBuffer splitKey, ByteBuffer page);

    void removeLeafPage(ByteBuffer key);
}
