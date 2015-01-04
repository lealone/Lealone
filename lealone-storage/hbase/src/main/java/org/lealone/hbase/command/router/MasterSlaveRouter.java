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
package org.lealone.hbase.command.router;

import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.dml.Delete;
import org.lealone.command.dml.Insert;
import org.lealone.command.dml.Merge;
import org.lealone.command.dml.Select;
import org.lealone.command.dml.Update;
import org.lealone.command.router.Router;
import org.lealone.hbase.command.dml.UpdateOrDeleteSupport;
import org.lealone.result.ResultInterface;

public class MasterSlaveRouter implements Router {
    private static final MasterSlaveRouter INSTANCE = new MasterSlaveRouter();

    public static MasterSlaveRouter getInstance() {
        return INSTANCE;
    }

    private MasterSlaveRouter() {
    }

    @Override
    public int executeDefineCommand(DefineCommand defineCommand) {
        return defineCommand.updateLocal();
    }

    @Override
    public int executeInsert(Insert insert) {
        return insert.updateLocal();
    }

    @Override
    public int executeMerge(Merge merge) {
        return merge.updateLocal();
    }

    @Override
    public int executeDelete(Delete delete) {
        return new UpdateOrDeleteSupport(delete).execute();
    }

    @Override
    public int executeUpdate(Update update) {
        return new UpdateOrDeleteSupport(update).execute();
    }

    @Override
    public ResultInterface executeSelect(Select select, int maxRows, boolean scrollable) {
        return select.queryLocal(maxRows);
    }

}
