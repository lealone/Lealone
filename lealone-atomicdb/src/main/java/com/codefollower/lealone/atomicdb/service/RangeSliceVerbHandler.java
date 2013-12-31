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
package com.codefollower.lealone.atomicdb.service;

import com.codefollower.lealone.atomicdb.db.AbstractRangeCommand;
import com.codefollower.lealone.atomicdb.db.RangeSliceReply;
import com.codefollower.lealone.atomicdb.db.filter.TombstoneOverwhelmingException;
import com.codefollower.lealone.atomicdb.net.IVerbHandler;
import com.codefollower.lealone.atomicdb.net.MessageIn;
import com.codefollower.lealone.atomicdb.net.MessagingService;
import com.codefollower.lealone.atomicdb.tracing.Tracing;

public class RangeSliceVerbHandler implements IVerbHandler<AbstractRangeCommand>
{
    public void doVerb(MessageIn<AbstractRangeCommand> message, int id)
    {
        try
        {
            if (StorageService.instance.isBootstrapMode())
            {
                /* Don't service reads! */
                throw new RuntimeException("Cannot service reads while bootstrapping!");
            }
            RangeSliceReply reply = new RangeSliceReply(message.payload.executeLocally());
            Tracing.trace("Enqueuing response to {}", message.from);
            MessagingService.instance().sendReply(reply.createMessage(), id, message.from);
        }
        catch (TombstoneOverwhelmingException e)
        {
            // error already logged.  Drop the request
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
