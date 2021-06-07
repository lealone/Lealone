/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.replication;

import java.util.HashSet;
import java.util.Random;

import org.lealone.db.Command;

abstract class ReplicationCommand<T extends ReplicaCommand> implements Command {

    private static final Random random = new Random(System.currentTimeMillis());

    protected final ReplicationSession session;
    protected final T[] commands;

    protected ReplicationCommand(ReplicationSession session, T[] commands) {
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
