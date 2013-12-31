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
package com.codefollower.lealone.atomicdb.db;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codefollower.lealone.atomicdb.config.Schema;
import com.codefollower.lealone.atomicdb.net.IVerbHandler;
import com.codefollower.lealone.atomicdb.net.MessageIn;
import com.codefollower.lealone.atomicdb.net.MessageOut;
import com.codefollower.lealone.atomicdb.net.MessagingService;
import com.codefollower.lealone.atomicdb.utils.UUIDSerializer;

public class SchemaCheckVerbHandler implements IVerbHandler
{
    private final Logger logger = LoggerFactory.getLogger(SchemaCheckVerbHandler.class);

    public void doVerb(MessageIn message, int id)
    {
        logger.debug("Received schema check request.");
        MessageOut<UUID> response = new MessageOut<UUID>(MessagingService.Verb.INTERNAL_RESPONSE, Schema.instance.getVersion(), UUIDSerializer.serializer);
        MessagingService.instance().sendReply(response, id, message.from);
    }
}
