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
package org.lealone.command;

import java.io.IOException;
import java.util.ArrayList;

import org.lealone.engine.SessionRemote;
import org.lealone.expression.ParameterInterface;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;
import org.lealone.value.Transfer;
import org.lealone.value.Value;

public class FrontendBatchCommand implements CommandInterface {
    private SessionRemote session;
    private ArrayList<Transfer> transferList;
    private ArrayList<String> batchCommands; //对应JdbcStatement.executeBatch()
    private ArrayList<Value[]> batchParameters; //对应JdbcPreparedStatement.executeBatch()
    private int id = -1;
    private int[] result;

    public FrontendBatchCommand(SessionRemote session, ArrayList<Transfer> transferList, ArrayList<String> batchCommands) {
        this.session = session;
        this.transferList = transferList;
        this.batchCommands = batchCommands;
    }

    public FrontendBatchCommand(SessionRemote session, ArrayList<Transfer> transferList, CommandInterface preparedCommand,
            ArrayList<Value[]> batchParameters) {
        this.session = session;
        this.transferList = transferList;
        this.batchParameters = batchParameters;

        if (preparedCommand instanceof CommandRemote)
            id = ((CommandRemote) preparedCommand).getId();
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
    public ResultInterface executeQuery(int maxRows, boolean scrollable) {
        throw DbException.throwInternalError();
    }

    @Override
    public int executeUpdate() {
        if (id == -1)
            id = session.getNextId();

        for (int i = 0, count = 0; i < transferList.size(); i++) {
            try {
                Transfer transfer = transferList.get(i);
                if (batchCommands != null) {
                    session.traceOperation("COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT", id);
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_BATCH_UPDATE_STATEMENT);
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
                    transfer.writeInt(SessionRemote.COMMAND_EXECUTE_BATCH_UPDATE_PREPAREDSTATEMENT).writeInt(id);
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
                session.removeServer(e, i--, ++count);
            }
        }

        return 0;
    }

    @Override
    public void close() {
        if (session == null || session.isClosed()) {
            return;
        }
        session = null;
        //不能clear，否则把SessionRemote中的transferList也clear了，
        //会导致JdbcSQLException: Database is already closed
        //transferList.clear();
        transferList = null;

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
    public ResultInterface getMetaData() {
        throw DbException.throwInternalError();
    }

    public int[] getResult() {
        return result;
    }
}
