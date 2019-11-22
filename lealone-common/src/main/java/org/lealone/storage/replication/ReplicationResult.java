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
package org.lealone.storage.replication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import org.lealone.common.exceptions.DbException;

public class ReplicationResult {

    private final ReplicationSession session;
    private final HashMap<ReplicaCommand, AtomicLong> results;

    public ReplicationResult(ReplicationSession session, ReplicaCommand[] commands) {
        this.session = session;
        results = new HashMap<>(commands.length);
        for (ReplicaCommand c : commands) {
            results.put(c, new AtomicLong(-1));
        }
    }

    public void addResult(ReplicaCommand command, long result) {
        AtomicLong old = results.get(command);
        if (old == null) {
            DbException.throwInternalError();
        }
        old.set(result);
    }

    Object validate(Object results) {
        validate();
        return null;
    }

    void validate() {
        final int n = session.n; // 复制集群节点总个数
        final int w = session.w; // 写成功的最少节点个数
        final boolean autoCommit = session.isAutoCommit();

        HashMap<Long, ArrayList<ReplicaCommand>> groupResults = new HashMap<>(1);
        for (Entry<ReplicaCommand, AtomicLong> e : results.entrySet()) {
            long v = e.getValue().get();
            if (v != -1) {
                ArrayList<ReplicaCommand> group = groupResults.get(v);
                if (group == null) {
                    group = new ArrayList<>(n);
                    groupResults.put(v, group);
                }
                group.add(e.getKey());
            }
        }
        boolean successful = false;
        long validKey = -1;
        ArrayList<ReplicaCommand> validNodes = null;
        ArrayList<ReplicaCommand> invalidNodes = new ArrayList<>(n);
        for (Entry<Long, ArrayList<ReplicaCommand>> e : groupResults.entrySet()) {
            ArrayList<ReplicaCommand> nodes = e.getValue();
            if (nodes.size() >= w) {
                successful = true;
                validKey = e.getKey();
                validNodes = e.getValue();
            } else {
                invalidNodes.addAll(nodes);
            }
        }
        if (successful) {
            if (validNodes.size() == n) {
                for (ReplicaCommand c : results.keySet()) {
                    c.replicaCommit(-1, autoCommit);
                }
            } else {
                if (validNodes != null) {
                    for (ReplicaCommand c : validNodes) {
                        c.replicaCommit(-1, autoCommit);
                    }
                }
                for (ReplicaCommand c : invalidNodes) {
                    c.replicaCommit(validKey, autoCommit);
                }
            }
        } else {
            for (ReplicaCommand c : results.keySet()) {
                c.replicaRollback();
            }
        }
    }
}
