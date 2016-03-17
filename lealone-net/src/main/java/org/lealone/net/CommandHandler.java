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
import java.util.concurrent.LinkedBlockingQueue;

import org.lealone.sql.SQLEngineManager;
import org.lealone.sql.SQLStatementExecutor;

public class CommandHandler extends Thread implements SQLStatementExecutor {

    private static final LinkedList<AsyncConnection> connections = new LinkedList<>();
    private static final PreparedCommand dummyCommand = new PreparedCommand(0, null, null, null);
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
            AsyncConnection ac = getNextBestAsyncConnection();
            if (ac == null)
                break;
            ac.executeOneCommand();
        }
    }

    private AsyncConnection getNextBestAsyncConnection() {
        if (connections.isEmpty())
            return null;

        double cost = 0.0;
        AsyncConnection ac = null;
        AsyncConnection lastAc = null;

        PreparedCommand pc;

        for (int i = 0, size = connections.size(); i < size; i++) {
            ac = connections.get(i);
            pc = ac.preparedCommandQueue.peek();
            if (pc == null)
                continue;

            if (pc.session.containsTransaction()) {
                return ac;
            }

            if (lastAc == null || pc.stmt.getCost() < cost) {
                lastAc = ac;
                cost = pc.stmt.getCost();
            }
        }

        if (lastAc == null)
            return null;

        return lastAc;
    }

    public static void addConnection(AsyncConnection c) {
        connections.add(c);
    }

    public static void removeConnection(AsyncConnection c) {
        connections.remove(c);
    }

}
