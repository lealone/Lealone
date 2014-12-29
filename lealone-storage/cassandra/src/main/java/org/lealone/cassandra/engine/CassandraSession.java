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
package org.lealone.cassandra.engine;

import java.util.Locale;

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.lealone.cassandra.command.CassandraCommand;
import org.lealone.cassandra.command.CassandraParser;
import org.lealone.command.Command;
import org.lealone.command.Parser;
import org.lealone.dbobject.User;
import org.lealone.engine.Database;
import org.lealone.engine.Session;
import org.lealone.message.DbException;

public class CassandraSession extends Session {

    private ClientState clientState;

    public CassandraSession(Database database, User user, int id) {
        super(database, user, id);

        clientState = ClientState.forInternalCalls();
        try {
            if (!Auth.isExistingUser(user.getName()))
                Auth.insertUser(user.getName(), user.isAdmin());
            clientState.login(new AuthenticatedUser(user.getName()));
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public Parser createParser() {
        return new CassandraParser(this);
    }

    @Override
    public synchronized Command prepareCommand(String sql) {
        String s = sql.toLowerCase(Locale.US);
        if (!(s.startsWith("create") || s.startsWith("alter") || s.startsWith("drop") || s.startsWith("use") || //
                s.startsWith("select") || s.startsWith("insert") || s.startsWith("delete") || s.startsWith("update")))
            return super.prepareCommand(sql);

        try {
            ResultMessage.Prepared prepared = ClientState.getCQLQueryHandler().prepare(sql, new QueryState(clientState));

            CassandraCommand command = new CassandraCommand(this, sql, prepared, clientState);
            return command;
        } catch (RequestValidationException e) {
            throw DbException.convert(e);
        }

    }
}
