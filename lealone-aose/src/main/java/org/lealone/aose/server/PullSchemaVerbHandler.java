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

import java.util.ArrayList;

import org.lealone.aose.gms.EchoVerbHandler;
import org.lealone.aose.net.IVerbHandler;
import org.lealone.aose.net.MessageIn;
import org.lealone.aose.net.MessageOut;
import org.lealone.aose.net.MessagingService;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.db.Database;
import org.lealone.db.DatabaseEngine;

public class PullSchemaVerbHandler implements IVerbHandler<PullSchema> {
    private static final Logger logger = LoggerFactory.getLogger(EchoVerbHandler.class);

    @Override
    public void doVerb(MessageIn<PullSchema> message, int id) {
        ArrayList<String> sqls = new ArrayList<>();
        for (Database db : DatabaseEngine.getDatabases()) {
            if (!db.isPersistent())
                continue;

            String sql = db.getCreateSQL();
            if (sql != null)
                sqls.add(sql);
        }
        MessageOut<PullSchemaAck> pullSchemaAck = new MessageOut<>(MessagingService.Verb.PULL_SCHEMA_ACK,
                new PullSchemaAck(sqls), PullSchemaAck.serializer);
        if (logger.isTraceEnabled())
            logger.trace("Sending a PullSchema reply {}", message.from);
        MessagingService.instance().sendReply(pullSchemaAck, id, message.from);
    }
}
