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
package org.lealone.storage;

import java.nio.ByteBuffer;

import org.lealone.db.Command;
import org.lealone.storage.replication.ReplicationResult;

public interface StorageCommand extends Command {

    Object executePut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value, boolean raw);

    Object executeGet(String mapName, ByteBuffer key);

    Object executeAppend(String replicationName, String mapName, ByteBuffer value,
            ReplicationResult replicationResult);

    LeafPageMovePlan prepareMoveLeafPage(String mapName, LeafPageMovePlan leafPageMovePlan);

    void moveLeafPage(String mapName, PageKey pageKey, ByteBuffer page, boolean addPage);

    void replicateRootPages(String dbName, ByteBuffer rootPages);

    void removeLeafPage(String mapName, PageKey pageKey);

    default ByteBuffer readRemotePage(String mapName, PageKey pageKey) {
        return null;
    }
}
