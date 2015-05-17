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
package org.lealone.command.ddl;

import java.util.Map;

import org.lealone.command.CommandInterface;
import org.lealone.dbobject.Schema;
import org.lealone.engine.Database;
import org.lealone.engine.Session;

public class AlterSchemaWithReplication extends DefineCommand {

    private Schema schema;
    private Map<String, String> replicationProperties;

    public AlterSchemaWithReplication(Session session) {
        super(session);
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public int update() {
        session.commit(true);
        Database db = session.getDatabase();
        session.getUser().checkAdmin();
        schema.setReplicationProperties(replicationProperties);
        db.update(session, schema);
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_SCHEMA_WTIH_REPLICATION;
    }

    public void setReplicationProperties(Map<String, String> replicationProperties) {
        this.replicationProperties = replicationProperties;
    }

}
