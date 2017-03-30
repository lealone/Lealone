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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import org.lealone.common.util.New;
import org.lealone.db.Command;
import org.lealone.db.CommandBase;
import org.lealone.db.CommandParameter;
import org.lealone.db.CommandUpdateResult;
import org.lealone.db.result.Result;
import org.lealone.replication.exceptions.ReadFailureException;
import org.lealone.replication.exceptions.ReadTimeoutException;
import org.lealone.replication.exceptions.WriteFailureException;
import org.lealone.replication.exceptions.WriteTimeoutException;
import org.lealone.storage.StorageCommand;

public class ReplicationCommand extends CommandBase implements StorageCommand {

    private static final Random random = new Random(System.currentTimeMillis());

    private final ReplicationSession session;
    private final Command[] commands;

    public ReplicationCommand(ReplicationSession session, Command[] commands) {
        this.session = session;
        this.commands = commands;
    }

    private Command getRandomNode(HashSet<Command> seen) {
        while (true) {
            // 随机选择一个节点，但是不能跟前面选过的重复
            Command c = commands[random.nextInt(session.n)];
            if (seen.add(c)) {
                return c;
            }

            if (seen.size() == session.n)
                return null;
        }
    }

    @Override
    public int getType() {
        return commands[0].getType();
    }

    @Override
    public boolean isQuery() {
        return commands[0].isQuery();
    }

    @Override
    public List<? extends CommandParameter> getParameters() {
        return commands[0].getParameters();
    }

    @Override
    public Result executeQuery(int maxRows) {
        return executeQuery(maxRows, false);
    }

    @Override
    public Result executeQuery(final int maxRows, final boolean scrollable) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        final ArrayList<Runnable> commands = New.arrayList(r);
        final HashSet<Command> seen = new HashSet<>();
        final ReadResponseHandler readResponseHandler = new ReadResponseHandler(n);
        final ArrayList<Exception> exceptions = New.arrayList(1);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            final Command c = getRandomNode(seen);
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    Result result = null;
                    try {
                        result = c.executeQuery(maxRows, scrollable);
                        readResponseHandler.response(result);
                    } catch (Exception e) {
                        exceptions.add(e);
                        if (readResponseHandler != null) {
                            readResponseHandler.onFailure();
                            Command c = getRandomNode(seen);
                            if (c != null) {
                                result = c.executeQuery(maxRows, scrollable);
                                readResponseHandler.response(result);
                                return;
                            }
                        }
                    }
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < r; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }

        try {
            return readResponseHandler.get(session.rpcTimeoutMillis);
        } catch (ReadTimeoutException | ReadFailureException e) {
            if (!exceptions.isEmpty())
                e.initCause(exceptions.get(0));
            throw e;
        }
    }

    @Override
    public int executeUpdate() {
        return executeUpdate(1);
    }

    @Override
    public int executeUpdate(String replicationName, CommandUpdateResult commandUpdateResult) {
        return executeUpdate();
    }

    private int executeUpdate(int tries) {
        int n = session.n;
        final String rn = session.createReplicationName();
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Runnable> commands = New.arrayList(n);
        final ArrayList<Exception> exceptions = New.arrayList(1);
        final CommandUpdateResult commandUpdateResult = new CommandUpdateResult(session.n, session.w,
                session.isAutoCommit(), this.commands);

        for (int i = 0; i < n; i++) {
            final Command c = this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        writeResponseHandler.response(c.executeUpdate(rn, commandUpdateResult));
                    } catch (Exception e) {
                        if (writeResponseHandler != null)
                            writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < n; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }

        try {
            writeResponseHandler.getUpdateCount(session.rpcTimeoutMillis);
            commandUpdateResult.validate();
            return commandUpdateResult.getUpdateCount();
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxRries)
                return executeUpdate(++tries);
            else {
                if (!exceptions.isEmpty())
                    e.initCause(exceptions.get(0));
                throw e;
            }
        }
    }

    @Override
    public void close() {
        for (Command c : commands)
            c.close();
    }

    @Override
    public void cancel() {
        for (Command c : commands)
            c.cancel();
    }

    @Override
    public Result getMetaData() {
        return commands[0].getMetaData();
    }

    @Override
    public Object executePut(String replicationName, String mapName, ByteBuffer key, ByteBuffer value) {
        return executePut(mapName, key, value, 1);
    }

    private Object executePut(final String mapName, final ByteBuffer key, final ByteBuffer value, int tries) {
        int n = session.n;
        final String rn = session.createReplicationName();
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Runnable> commands = New.arrayList(n);
        final ArrayList<Exception> exceptions = New.arrayList(1);

        for (int i = 0; i < n; i++) {
            final StorageCommand c = (StorageCommand) this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        writeResponseHandler.response(c.executePut(rn, mapName, key.slice(), value.slice()));
                    } catch (Exception e) {
                        if (writeResponseHandler != null)
                            writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < n; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }

        try {
            return writeResponseHandler.getResult(session.rpcTimeoutMillis);
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxRries) {
                key.rewind();
                value.rewind();
                return executePut(mapName, key, value, ++tries);
            } else {
                if (!exceptions.isEmpty())
                    e.initCause(exceptions.get(0));
                throw e;
            }
        }
    }

    @Override
    public Object executeGet(final String mapName, final ByteBuffer key) {
        int n = session.n;
        int r = session.r;
        r = 1; // 使用Write all read one模式
        final ArrayList<Runnable> commands = New.arrayList(r);
        final HashSet<Command> seen = new HashSet<>();
        final ReadResponseHandler readResponseHandler = new ReadResponseHandler(n);
        final ArrayList<Exception> exceptions = New.arrayList(1);

        // 随机选择R个节点并行读，如果读不到再试其他节点
        for (int i = 0; i < r; i++) {
            final StorageCommand c = (StorageCommand) getRandomNode(seen);
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    Object result = null;
                    try {
                        result = c.executeGet(mapName, key);
                        readResponseHandler.response(result);
                    } catch (Exception e) {
                        if (readResponseHandler != null) {
                            readResponseHandler.onFailure();
                            StorageCommand c = (StorageCommand) getRandomNode(seen);
                            if (c != null) {
                                result = c.executeGet(mapName, key);
                                readResponseHandler.response(result);
                                return;
                            }
                        }
                        exceptions.add(e);
                    }
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < r; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }

        try {
            return readResponseHandler.getResultObject(session.rpcTimeoutMillis);
        } catch (ReadTimeoutException | ReadFailureException e) {
            if (!exceptions.isEmpty())
                e.initCause(exceptions.get(0));
            throw e;
        }
    }

    @Override
    public void moveLeafPage(final String mapName, final ByteBuffer splitKey, final ByteBuffer page) {
        int n = session.n;
        final ArrayList<Runnable> commands = New.arrayList(n);
        for (int i = 0; i < n; i++) {
            final StorageCommand c = (StorageCommand) this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    c.moveLeafPage(mapName, splitKey, page);
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < n; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }
    }

    @Override
    public void removeLeafPage(final String mapName, final ByteBuffer key) {
        int n = session.n;
        final ArrayList<Runnable> commands = New.arrayList(n);
        for (int i = 0; i < n; i++) {
            final StorageCommand c = (StorageCommand) this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    c.removeLeafPage(mapName, key);
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < n; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }
    }

    @Override
    public Command prepare() {
        return this;
    }

    @Override
    public Object executeAppend(String replicationName, String mapName, ByteBuffer value,
            CommandUpdateResult commandUpdateResult) {
        return executeAppend(mapName, value, 1);
    }

    private Object executeAppend(final String mapName, final ByteBuffer value, int tries) {
        int n = session.n;
        final String rn = session.createReplicationName();
        final WriteResponseHandler writeResponseHandler = new WriteResponseHandler(n);
        final ArrayList<Runnable> commands = New.arrayList(n);
        final ArrayList<Exception> exceptions = New.arrayList(1);
        final CommandUpdateResult commandUpdateResult = new CommandUpdateResult(session.n, session.w,
                session.isAutoCommit(), this.commands);

        for (int i = 0; i < n; i++) {
            final StorageCommand c = (StorageCommand) this.commands[i];
            Runnable command = new Runnable() {
                @Override
                public void run() {
                    try {
                        writeResponseHandler.response(c.executeAppend(rn, mapName, value.slice(), commandUpdateResult));
                    } catch (Exception e) {
                        if (writeResponseHandler != null)
                            writeResponseHandler.onFailure();
                        exceptions.add(e);
                    }
                }
            };
            commands.add(command);
        }

        for (int i = 0; i < n; i++) {
            ThreadPool.executor.submit(commands.get(i));
        }

        try {
            Object result = writeResponseHandler.getResult(session.rpcTimeoutMillis);
            commandUpdateResult.validate();
            return result;
        } catch (WriteTimeoutException | WriteFailureException e) {
            if (tries < session.maxRries) {
                value.rewind();
                return executeAppend(mapName, value, ++tries);
            } else {
                if (!exceptions.isEmpty())
                    e.initCause(exceptions.get(0));
                throw e;
            }
        }
    }
}
