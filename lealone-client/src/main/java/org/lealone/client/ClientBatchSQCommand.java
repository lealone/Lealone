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

import org.lealone.common.exceptions.DbException;
import org.lealone.db.Command;
import org.lealone.db.CommandParameter;
import org.lealone.db.CommandUpdateResult;
import org.lealone.db.Session;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.net.AsyncCallback;
import org.lealone.net.TransferInputStream;
import org.lealone.net.TransferOutputStream;
import org.lealone.sql.SQLCommand;

public class ClientBatchSQCommand implements SQLCommand {

    private ClientSession session;
    private ArrayList<String> batchCommands; // 对应JdbcStatement.executeBatch()
    private ArrayList<Value[]> batchParameters; // 对应JdbcPreparedStatement.executeBatch()
    private int packetId = -1;
    private int[] result;

    public ClientBatchSQCommand(ClientSession session, ArrayList<String> batchCommands) {
        this.session = session;
        this.batchCommands = batchCommands;
    }

    public ClientBatchSQCommand(ClientSession session, Command preparedCommand, ArrayList<Value[]> batchParameters) {
        this.session = session;
        this.batchParameters = batchParameters;

        if (preparedCommand instanceof ClientSQLCommand)
            packetId = ((ClientSQLCommand) preparedCommand).getId();
    }

    @Override
    public int getType() {
        return CLIENT_BATCH_SQL_COMMAND;
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
    public int executeUpdate(String replicationName, CommandUpdateResult commandUpdateResult) {
        return executeUpdate();
    }

    @Override
    public int executeUpdate() {
        if (packetId == -1)
            packetId = session.getNextId();

        try {
            TransferOutputStream out = session.newOut();
            if (batchCommands != null) {
                session.traceOperation("COMMAND_BATCH_STATEMENT_UPDATE", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_BATCH_STATEMENT_UPDATE);
                int size = batchCommands.size();
                result = new int[size];
                out.writeInt(size);
                for (int i = 0; i < size; i++) {
                    out.writeString(batchCommands.get(i));
                }
                getResultAsync(out);
            } else {
                session.traceOperation("COMMAND_BATCH_STATEMENT_PREPARED_UPDATE", packetId);
                out.writeRequestHeader(packetId, Session.COMMAND_BATCH_STATEMENT_PREPARED_UPDATE);
                int size = batchParameters.size();
                result = new int[size];
                out.writeInt(size);
                Value[] values;
                int len;
                for (int i = 0; i < size; i++) {
                    values = batchParameters.get(i);
                    len = values.length;
                    for (int m = 0; m < len; m++)
                        out.writeValue(values[m]);
                }
                getResultAsync(out);
            }
        } catch (IOException e) {
            session.handleException(e);
        }
        return 0;
    }

    private void getResultAsync(TransferOutputStream out) throws IOException {
        out.flushAndAwait(packetId, new AsyncCallback<Void>() {
            @Override
            public void runInternal(TransferInputStream in) throws Exception {
                for (int i = 0, size = ClientBatchSQCommand.this.result.length; i < size; i++)
                    ClientBatchSQCommand.this.result[i] = in.readInt();
            }
        });
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        session.traceOperation("COMMAND_CLOSE", packetId);
        try {
            session.newOut().writeRequestHeader(packetId, Session.COMMAND_CLOSE).flush();
        } catch (IOException e) {
            session.getTrace().error(e, "close");
        }
        session = null;

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
        session.cancelStatement(packetId);
    }

    @Override
    public Result getMetaData() {
        throw DbException.throwInternalError();
    }

    public int[] getResult() {
        return result;
    }

    @Override
    public int getId() {
        return packetId;
    }
}
