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
package org.lealone.aose.server;

import org.lealone.aose.net.IVerbHandler;
import org.lealone.aose.net.MessageIn;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.ServerSession;

public class PullSchemaAckVerbHandler implements IVerbHandler<PullSchemaAck> {
    @Override
    public void doVerb(MessageIn<PullSchemaAck> message, int id) {
        ServerSession s = LealoneDatabase.getInstance().getSystemSession();
        for (String sql : message.payload.sqls)
            s.prepareStatementLocal(sql).executeUpdate();

        P2pServer.instance.pullSchemaFinished = true;
    }
}
