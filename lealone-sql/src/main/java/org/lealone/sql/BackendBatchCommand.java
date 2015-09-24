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
import org.lealone.common.value.Value;
import org.lealone.db.CommandInterface;
import org.lealone.db.ParameterInterface;
import org.lealone.db.Session;
import org.lealone.db.result.ResultInterface;
import org.lealone.sql.expression.Parameter;

public class BackendBatchCommand implements CommandInterface {
    private Session session;
    private ArrayList<String> batchCommands; // 对应JdbcStatement.executeBatch()
    private ArrayList<Value[]> batchParameters; // 对应JdbcPreparedStatement.executeBatch()
    private Command preparedCommand;
    private int[] result;

    public BackendBatchCommand(Session session, ArrayList<String> batchCommands) {
        this.session = session;
        this.batchCommands = batchCommands;
    }

    public BackendBatchCommand(Session session, Command preparedCommand, ArrayList<Value[]> batchParameters) {
        this.session = session;
        this.preparedCommand = preparedCommand;
        this.batchParameters = batchParameters;
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
        if (batchCommands != null) {
            int size = batchCommands.size();
            result = new int[size];
            for (int i = 0; i < size; i++) {
                Command c = session.prepareCommand(batchCommands.get(i));
                result[i] = c.executeUpdate();
                c.close();
            }
        } else {
            int size = batchParameters.size();
            result = new int[size];
            Value[] values;
            ArrayList<? extends ParameterInterface> params = preparedCommand.getParameters();
            int paramsSize = params.size();
            for (int i = 0; i < size; i++) {
                values = batchParameters.get(i);
                for (int j = 0; j < paramsSize; j++) {
                    Parameter p = (Parameter) params.get(j);
                    p.setValue(values[j], true);
                }
                result[i] = preparedCommand.executeUpdate();
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
    public ResultInterface getMetaData() {
        throw DbException.throwInternalError();
    }

    public int[] getResult() {
        return result;
    }

}
