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

import java.util.HashSet;
import java.util.Random;

import org.lealone.db.Command;

public abstract class ReplicationCommand<T extends Command> implements Command {

    private static final Random random = new Random(System.currentTimeMillis());

    protected final ReplicationSession session;
    protected final T[] commands;

    public ReplicationCommand(ReplicationSession session, T[] commands) {
        this.session = session;
        this.commands = commands;
    }

    protected T getRandomNode(HashSet<T> seen) {
        while (true) {
            // 随机选择一个节点，但是不能跟前面选过的重复
            T c = commands[random.nextInt(session.n)];
            if (seen.add(c)) {
                return c;
            }
            if (seen.size() == session.n)
                return null;
        }
    }

    @Override
    public void cancel() {
        for (T c : commands)
            c.cancel();
    }

    @Override
    public void close() {
        for (T c : commands)
            c.close();
    }
}
