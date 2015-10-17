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
package org.lealone.sql;

import java.util.ArrayList;

import org.lealone.common.message.DbException;
import org.lealone.db.CommandParameter;
import org.lealone.db.ServerSession;
import org.lealone.db.result.Result;
import org.lealone.db.value.Value;
import org.lealone.sql.expression.Parameter;

public class BatchStatementImpl implements BatchStatement {
    private ServerSession session;
    private ArrayList<String> batchCommands; // 对应JdbcStatement.executeBatch()
    private ArrayList<Value[]> batchParameters; // 对应JdbcPreparedStatement.executeBatch()
    private StatementBase preparedCommand;
    private int[] result;

    public BatchStatementImpl(ServerSession session, ArrayList<String> batchCommands) {
        this.session = session;
        this.batchCommands = batchCommands;
    }

    public BatchStatementImpl(ServerSession session, StatementBase preparedCommand, ArrayList<Value[]> batchParameters) {
        this.session = session;
        this.preparedCommand = preparedCommand;
        this.batchParameters = batchParameters;
    }

    @Override
    public boolean isQuery() {
        return false;
    }

    @Override
    public ArrayList<? extends CommandParameter> getParameters() {
        throw DbException.throwInternalError();
    }

    @Override
    public Result executeQuery(int maxRows, boolean scrollable) {
        throw DbException.throwInternalError();
    }

    @Override
    public int executeUpdate() {
        if (batchCommands != null) {
            int size = batchCommands.size();
            result = new int[size];
            for (int i = 0; i < size; i++) {
                StatementBase c = (StatementBase) session.prepareStatement(batchCommands.get(i), -1);
                result[i] = c.update();
                c.close();
            }
        } else {
            int size = batchParameters.size();
            result = new int[size];
            Value[] values;
            ArrayList<? extends CommandParameter> params = preparedCommand.getParameters();
            int paramsSize = params.size();
            for (int i = 0; i < size; i++) {
                values = batchParameters.get(i);
                for (int j = 0; j < paramsSize; j++) {
                    Parameter p = (Parameter) params.get(j);
                    p.setValue(values[j], true);
                }
                result[i] = preparedCommand.update();
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
    }

    @Override
    public Result getMetaData() {
        throw DbException.throwInternalError();
    }

    @Override
    public int[] getResult() {
        return result;
    }

    @Override
    public int getType() {
        return 0;
    }

}
