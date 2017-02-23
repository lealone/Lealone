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
package org.lealone.aose.gms;

import org.lealone.aose.net.IVerbHandler;
import org.lealone.aose.net.MessageIn;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

public class GossipShutdownVerbHandler implements IVerbHandler<Void> {
    private static final Logger logger = LoggerFactory.getLogger(GossipShutdownVerbHandler.class);

    @Override
    public void doVerb(MessageIn<Void> message, int id) {
        if (!Gossiper.instance.isEnabled()) {
            if (logger.isDebugEnabled())
                logger.debug("Ignoring shutdown message from {} because gossip is disabled", message.from);
            return;
        }
        FailureDetector.instance.forceConviction(message.from);
    }
}