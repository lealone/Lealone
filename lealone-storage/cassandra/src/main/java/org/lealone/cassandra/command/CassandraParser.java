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
package org.lealone.cassandra.command;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.cassandra.cql3.statements.KSPropDefs;
import org.apache.cassandra.exceptions.SyntaxException;
import org.lealone.cassandra.command.ddl.AlterKeyspace;
import org.lealone.cassandra.command.ddl.CreateKeyspace;
import org.lealone.cassandra.command.ddl.DropKeyspace;
import org.lealone.cassandra.engine.CassandraSession;
import org.lealone.command.Parser;
import org.lealone.command.Prepared;
import org.lealone.engine.Session;
import org.lealone.expression.Expression;

public class CassandraParser extends Parser {
    private final CassandraSession session;

    public CassandraParser(Session session) {
        super(session);
        this.session = (CassandraSession) session;
    }

    @Override
    protected Prepared parseCreate() {
        if (readIf("SCHEMA") || readIf("KEYSPACE")) {
            return parseCreateKeyspace();
        } else {
            return super.parseCreate();
        }
    }

    @Override
    protected Prepared parseAlter() {
        if (readIf("SCHEMA") || readIf("KEYSPACE")) {
            return parseAlterKeyspace();
        } else {
            return super.parseAlter();
        }
    }

    @Override
    protected Prepared parseDrop() {
        if (readIf("SCHEMA") || readIf("KEYSPACE")) {
            return parseDropKeyspace();
        } else {
            return super.parseDrop();
        }
    }

    private Map<String, String> parseMap() {
        Map<String, String> map = new HashMap<String, String>();
        do {
            String name = readString();//readUniqueIdentifier();
            String value = null;
            if (readIf(":")) {
                value = readString();
            }
            map.put(name, value);
        } while (readIfMore("}"));

        return map;
    }

    private boolean readIfMore(String end) {
        if (readIf(",")) {
            return !readIf(end);
        }
        read(end);
        return false;
    }

    private KSPropDefs parseKSPropDefs() {
        KSPropDefs defs = new KSPropDefs();
        try {
            read("WITH");
            do {
                String name = readUniqueIdentifier().toLowerCase(Locale.US);
                read("=");
                if (readIf("{")) {
                    defs.addProperty(name, parseMap());
                } else {
                    Expression value = readExpression();
                    defs.addProperty(name, value.optimize(session).getValue(session).toString());
                }
            } while (readIf("AND"));

            defs.validate();
        } catch (SyntaxException e) {
            throw getSyntaxError();
        }
        return defs;
    }

    protected Prepared parseCreateKeyspace() {
        CreateKeyspace command = new CreateKeyspace(session);

        command.setIfNotExists(readIfNoExists());
        command.setKeyspaceName(readUniqueIdentifier());

        if (readIf("AUTHORIZATION")) {
            command.setAuthorization(readUniqueIdentifier());
        } else {
            command.setAuthorization(session.getUser().getName());
        }
        KSPropDefs defs = parseKSPropDefs();
        command.setKSPropDefs(defs);
        return command;
    }

    protected Prepared parseAlterKeyspace() {
        AlterKeyspace command = new AlterKeyspace(session);
        command.setKeyspaceName(readUniqueIdentifier());

        KSPropDefs defs = parseKSPropDefs();
        command.setKSPropDefs(defs);
        command.validate();
        return command;
    }

    protected Prepared parseDropKeyspace() {
        DropKeyspace command = new DropKeyspace(session);

        command.setIfExists(readIfExists(false));
        command.setKeyspaceName(readUniqueIdentifier());

        return command;
    }

}
