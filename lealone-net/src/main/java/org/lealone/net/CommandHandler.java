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

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.lealone.db.SessionStatus;
import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLStatementExecutor;

public class CommandHandler extends Thread implements SQLStatementExecutor {

    private static final LinkedList<AsyncConnection> connections = new LinkedList<>();
    private static final PreparedCommand dummyCommand = new PreparedCommand(0, null, null, null, null);
    static final LinkedBlockingQueue<PreparedCommand> preparedCommandQueue = new LinkedBlockingQueue<>();

    private boolean stop;

    public CommandHandler() {
        super("CommandHandler");
    }

    @Override
    public void run() {
        SQLEngineManager.getInstance().setSQLStatementExecutor(this);
        while (!stop) {
            executeNextStatement();
        }
    }

    public void end() {
        stop = true;
        ready();
    }

    @Override
    public void ready() {
        preparedCommandQueue.add(dummyCommand);
    }

    @Override
    public void executeNextStatement() {
        try {
            preparedCommandQueue.take();
        } catch (InterruptedException e) {
        }
        preparedCommandQueue.clear();

        while (true) {
            PreparedCommand c = getNextBestCommand();
            if (c == null)
                break;
            try {
                c.run();
            } catch (Throwable e) {
                AsyncConnection.sendError(c.transfer, c.id, e);
            }
        }
    }

    private PreparedCommand getNextBestCommand() {
        if (connections.isEmpty())
            return null;

        AsyncConnection ac;
        PreparedCommand pc;
        ConcurrentLinkedQueue<PreparedCommand> bestPreparedCommandQueue = null;
        double cost = 0.0;

        outer: for (int i = 0, size = connections.size(); i < size; i++) {
            ac = connections.get(i);
            for (ConcurrentLinkedQueue<PreparedCommand> preparedCommandQueue : ac.preparedCommands.values()) {
                pc = preparedCommandQueue.peek();
                if (pc == null)
                    continue;

                SessionStatus sessionStatus = pc.session.getStatus();
                if (sessionStatus == SessionStatus.TRANSACTION_NOT_COMMIT) {
                    bestPreparedCommandQueue = preparedCommandQueue;
                    break outer;
                } else if (sessionStatus == SessionStatus.COMMITTING_TRANSACTION) {
                    continue;
                }

                if (bestPreparedCommandQueue == null || pc.stmt.getCost() < cost) {
                    bestPreparedCommandQueue = preparedCommandQueue;
                    cost = pc.stmt.getCost();
                }
            }
        }

        if (bestPreparedCommandQueue == null)
            return null;

        return bestPreparedCommandQueue.poll();
    }

    public static void addConnection(AsyncConnection c) {
        connections.add(c);
    }

    public static void removeConnection(AsyncConnection c) {
        connections.remove(c);
        c.close();
    }

}
