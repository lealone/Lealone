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
import org.lealone.db.Command;

public class ReplicationResult {

    private int updateCount;
    private final int n; // 复制集群节点总个数
    private final int w; // 写成功的最少节点个数
    private final boolean autoCommit;
    private final HashMap<Command, AtomicLong> results;

    public ReplicationResult(int n, int w, boolean autoCommit, Command[] commands) {
        this.n = n;
        this.w = w;
        this.autoCommit = autoCommit;
        results = new HashMap<>(commands.length);
        for (Command c : commands) {
            results.put(c, new AtomicLong(-1));
        }
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

    public void addResult(Command command, long result) {
        AtomicLong old = results.get(command);
        if (old == null) {
            DbException.throwInternalError();
        }
        old.set(result);
    }

    public void validate() {
        HashMap<Long, ArrayList<Command>> groupResults = new HashMap<>(1);
        for (Entry<Command, AtomicLong> e : results.entrySet()) {
            long v = e.getValue().get();
            if (v != -1) {
                ArrayList<Command> group = groupResults.get(v);
                if (group == null) {
                    group = new ArrayList<>(n);
                    groupResults.put(v, group);
                }
                group.add(e.getKey());
            }
        }
        boolean successful = false;
        long validKey = -1;
        ArrayList<Command> validNodes = null;
        ArrayList<Command> invalidNodes = new ArrayList<>(n);
        for (Entry<Long, ArrayList<Command>> e : groupResults.entrySet()) {
            ArrayList<Command> nodes = e.getValue();
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
                for (Command c : results.keySet()) {
                    c.replicationCommit(-1, autoCommit);
                }
            } else {
                if (validNodes != null) {
                    for (Command c : validNodes) {
                        c.replicationCommit(-1, autoCommit);
                    }
                }
                for (Command c : invalidNodes) {
                    c.replicationCommit(validKey, autoCommit);
                }
            }
        } else {
            for (Command c : results.keySet()) {
                c.replicationRollback();
            }
        }
    }
}
