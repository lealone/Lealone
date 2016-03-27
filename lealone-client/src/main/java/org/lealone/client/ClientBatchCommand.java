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
package org.lealone.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.lealone.async.AsyncHandler;
import org.lealone.async.AsyncResult;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.trace.Trace;
import org.lealone.db.Command;
import org.lealone.db.CommandParameter;
import org.lealone.db.Session;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.net.AsyncCallback;
import org.lealone.net.Transfer;

public class ClientBatchCommand implements Command {
    private ClientSession session;
    private Transfer transfer;
    private final Trace trace;
    private ArrayList<String> batchCommands; // 对应JdbcStatement.executeBatch()
    private ArrayList<Value[]> batchParameters; // 对应JdbcPreparedStatement.executeBatch()
    private int id = -1;
    private int[] result;

    public ClientBatchCommand(ClientSession session, Transfer transfer, ArrayList<String> batchCommands) {
        this.session = session;
        this.transfer = transfer;
        this.batchCommands = batchCommands;
        trace = session.getTrace();
    }

    public ClientBatchCommand(ClientSession session, Transfer transfer, Command preparedCommand,
            ArrayList<Value[]> batchParameters) {
        this.session = session;
        this.transfer = transfer;
        this.batchParameters = batchParameters;
        trace = session.getTrace();

        if (preparedCommand instanceof ClientCommand)
            id = ((ClientCommand) preparedCommand).getId();
    }

    @Override
    public int getType() {
        return CLIENT_BATCH_COMMAND;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public List<? extends CommandParameter> getParameters() {
        throw DbException.throwInternalError();
    }

    @Override
    public Result executeQuery(int maxRows) {
        return executeQuery(maxRows, false);
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable) {
        throw DbException.throwInternalError();
    }

    @Override
    public int executeUpdate(String replicationName) {
        return executeUpdate();
    }

    @Override
    public int executeUpdate() {
        if (id == -1)
            id = session.getNextId();

        try {
            if (batchCommands != null) {
                session.traceOperation("COMMAND_BATCH_STATEMENT_UPDATE", id);
                transfer.writeRequestHeader(id, Session.COMMAND_BATCH_STATEMENT_UPDATE);
                transfer.writeInt(session.getSessionId());
                int size = batchCommands.size();
                result = new int[size];
                transfer.writeInt(size);
                for (int i = 0; i < size; i++) {
                    transfer.writeString(batchCommands.get(i));
                }
                getResultAsync();
            } else {
                session.traceOperation("COMMAND_BATCH_STATEMENT_PREPARED_UPDATE", id);
                transfer.writeRequestHeader(id, Session.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE);
                transfer.writeInt(session.getSessionId());
                int size = batchParameters.size();
                result = new int[size];
                transfer.writeInt(size);
                Value[] values;
                int len;
                for (int i = 0; i < size; i++) {
                    values = batchParameters.get(i);
                    len = values.length;
                    for (int m = 0; m < len; m++)
                        transfer.writeValue(values[m]);
                }
                getResultAsync();
            }
        } catch (IOException e) {
            session.handleException(e);
        }

        return 0;
    }

    private void getResultAsync() throws IOException {
        AsyncCallback<Void> ac = new AsyncCallback<Void>() {
            @Override
            public void runInternal() {
                try {
                    for (int i = 0, size = ClientBatchCommand.this.result.length; i < size; i++)
                        ClientBatchCommand.this.result[i] = transfer.readInt();
                } catch (IOException e) {
                    throw DbException.convert(e);
                }
            }
        };
        transfer.addAsyncCallback(id, ac);
        transfer.flush();
        ac.await();
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        session.traceOperation("COMMAND_CLOSE", id);
        try {
            transfer.writeRequestHeader(id, Session.COMMAND_CLOSE).flush();
        } catch (IOException e) {
            trace.error(e, "close");
        }
        session = null;
        transfer = null;

        if (batchCommands != null) {
            batchCommands.clear();
            batchCommands = null;
        }
        if (batchParameters != null) {
            batchParameters.clear();
            batchParameters = null;
        }

        result = null;
    }

    @Override
    public void cancel() {
        session.cancelStatement(id);
    }

    @Override
    public Result getMetaData() {
        throw DbException.throwInternalError();
    }

    public int[] getResult() {
        return result;
    }

    @Override
    public Command prepare() {
        return this;
    }

    @Override
    public void executeQueryAsync(int maxRows, boolean scrollable, AsyncHandler<AsyncResult<Result>> handler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void executeUpdateAsync(AsyncHandler<AsyncResult<Integer>> handler) {
        // TODO Auto-generated method stub

    }

}
