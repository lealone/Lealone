/*
 * Copyright 2011 The Apache Software Foundation
 *
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

import org.lealone.common.message.DbException;
import org.lealone.db.CommandInterface;
import org.lealone.db.ParameterInterface;
import org.lealone.db.result.Result;
import org.lealone.db.value.Transfer;
import org.lealone.db.value.Value;

public class FrontendBatchCommand implements CommandInterface {
    private FrontendSession session;
    private Transfer transfer;
    private ArrayList<String> batchCommands; //对应JdbcStatement.executeBatch()
    private ArrayList<Value[]> batchParameters; //对应JdbcPreparedStatement.executeBatch()
    private int id = -1;
    private int[] result;

    public FrontendBatchCommand(FrontendSession session, Transfer transfer, ArrayList<String> batchCommands) {
        this.session = session;
        this.transfer = transfer;
        this.batchCommands = batchCommands;
    }

    public FrontendBatchCommand(FrontendSession session, Transfer transfer, CommandInterface preparedCommand,
            ArrayList<Value[]> batchParameters) {
        this.session = session;
        this.transfer = transfer;
        this.batchParameters = batchParameters;

        if (preparedCommand instanceof FrontendCommand)
            id = ((FrontendCommand) preparedCommand).getId();
    }

    @Override
    public int getCommandType() {
        return UNKNOWN;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        throw DbException.throwInternalError();
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable) {
        throw DbException.throwInternalError();
    }

    @Override
    public int executeUpdate() {
        if (id == -1)
            id = session.getNextId();

        try {
            if (batchCommands != null) {
                session.traceOperation("COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT", id);
                transfer.writeInt(FrontendSession.COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT);
                int size = batchCommands.size();
                result = new int[size];
                transfer.writeInt(size);
                for (int j = 0; j < size; j++)
                    transfer.writeString(batchCommands.get(j));
                session.done(transfer);

                for (int j = 0; j < size; j++)
                    result[j] = transfer.readInt();
            } else {
                session.traceOperation("COMMAND_EXECUTE_BATCH_UPDATE_PREPAREDSTATEMENT", id);
                transfer.writeInt(FrontendSession.COMMAND_EXECUTE_BATCH_UPDATE_PREPAREDSTATEMENT).writeInt(id);
                int size = batchParameters.size();
                result = new int[size];
                transfer.writeInt(size);
                Value[] values;
                int len;
                for (int j = 0; j < size; j++) {
                    values = batchParameters.get(j);
                    len = values.length;
                    for (int m = 0; m < len; m++)
                        transfer.writeValue(values[m]);
                }
                session.done(transfer);

                for (int j = 0; j < size; j++)
                    result[j] = transfer.readInt();
            }
        } catch (IOException e) {
            session.handleException(e);
        }

        return 0;
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
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
}
