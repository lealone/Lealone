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
package org.lealone.p2p.net;

import java.util.concurrent.TimeUnit;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class ResponseVerbHandler implements IVerbHandler {
    private static final Logger logger = LoggerFactory.getLogger(ResponseVerbHandler.class);

    @Override
    public void doVerb(MessageIn message, int id) {
        long latency = TimeUnit.NANOSECONDS
                .toMillis(System.nanoTime() - MessagingService.instance().getRegisteredCallbackAge(id));
        CallbackInfo callbackInfo = MessagingService.instance().removeRegisteredCallback(id);
        if (callbackInfo == null) {
            String msg = "Callback already removed for {} (from {})";
            if (logger.isDebugEnabled())
                logger.debug(msg, id, message.from);
            return;
        }

        IAsyncCallback cb = callbackInfo.callback;
        if (message.isFailureResponse()) {
            ((IAsyncCallbackWithFailure) cb).onFailure(message.from);
        } else {
            // TODO: Should we add latency only in success cases?
            MessagingService.instance().maybeAddLatency(cb, message.from, latency);
            cb.response(message);
        }
    }
}
