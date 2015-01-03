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
package org.lealone.cluster.router;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.lealone.command.CommandInterface;
import org.lealone.command.Prepared;
import org.lealone.command.ddl.DefineCommand;
import org.lealone.command.dml.Delete;
import org.lealone.command.dml.Insert;
import org.lealone.command.dml.Merge;
import org.lealone.command.dml.Select;
import org.lealone.command.dml.Update;
import org.lealone.command.router.Router;
import org.lealone.engine.Session;
import org.lealone.message.DbException;
import org.lealone.result.ResultInterface;

public class RDBMSRouter implements Router {
    private final P2PRouter p2pRouter;

    private final Connection conn;
    private final Statement stmt;

    public RDBMSRouter(String url, String user, String password) {
        this.p2pRouter = P2PRouter.getInstance();

        try {
            conn = DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public int executeDefineCommand(DefineCommand defineCommand) {
        return execute(defineCommand);
    }

    @Override
    public int executeInsert(Insert insert) {
        return execute(insert);
    }

    @Override
    public int executeMerge(Merge merge) {
        return execute(merge);
    }

    @Override
    public int executeDelete(Delete delete) {
        return execute(delete);
    }

    @Override
    public int executeUpdate(Update update) {
        return execute(update);
    }

    //TODO 合并本节点的结果
    @Override
    public ResultInterface executeSelect(Select select, int maxRows, boolean scrollable) {
        return p2pRouter.executeSelect(select, maxRows, scrollable);
    }

    private int execute(Prepared p) {
        int updateCount = 0;
        switch (p.getType()) {
        case CommandInterface.INSERT:
            updateCount = p2pRouter.executeInsert((Insert) p);
            break;
        case CommandInterface.UPDATE:
            updateCount = p2pRouter.executeUpdate((Update) p);
            break;
        case CommandInterface.DELETE:
            updateCount = p2pRouter.executeDelete((Delete) p);
            break;
        case CommandInterface.MERGE:
            updateCount = p2pRouter.executeMerge((Merge) p);
            break;
        default:
            if (p instanceof DefineCommand)
                updateCount = p2pRouter.executeDefineCommand((DefineCommand) p);
            break;
        }

        Session session = p.getSession();

        if (session.getConnectionInfo() != null && !session.getConnectionInfo().isEmbedded()
                && session.getDatabase().isPersistent()) {
            synchronized (conn) {
                try {
                    updateCount += stmt.executeUpdate(p.getSQL());
                } catch (SQLException e) {
                    throw DbException.convert(e);
                }
            }
        }
        return updateCount;
    }
}
