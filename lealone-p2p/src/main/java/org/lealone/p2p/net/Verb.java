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

import org.lealone.p2p.concurrent.Stage;
import org.lealone.p2p.gms.EchoMessage;
import org.lealone.p2p.gms.EchoVerbHandler;
import org.lealone.p2p.gms.GossipDigestAck;
import org.lealone.p2p.gms.GossipDigestAck2;
import org.lealone.p2p.gms.GossipDigestAck2VerbHandler;
import org.lealone.p2p.gms.GossipDigestAckVerbHandler;
import org.lealone.p2p.gms.GossipDigestSyn;
import org.lealone.p2p.gms.GossipDigestSynVerbHandler;
import org.lealone.p2p.gms.GossipShutdownVerbHandler;
import org.lealone.p2p.net.MessagingService.CallbackDeterminedSerializer;

/* All verb handler identifiers */
public enum Verb {
    // client-initiated reads and writes
    REQUEST_RESPONSE(
            Stage.REQUEST_RESPONSE, //
            CallbackDeterminedSerializer.instance, //
            new ResponseVerbHandler()),

    // responses to internal calls
    INTERNAL_RESPONSE(
            Stage.INTERNAL_RESPONSE, //
            CallbackDeterminedSerializer.instance, //
            new ResponseVerbHandler()),

    GOSSIP_DIGEST_SYN(
            Stage.GOSSIP, //
            GossipDigestSyn.serializer, //
            new GossipDigestSynVerbHandler()),

    GOSSIP_DIGEST_ACK(
            Stage.GOSSIP, //
            GossipDigestAck.serializer, //
            new GossipDigestAckVerbHandler()),

    GOSSIP_DIGEST_ACK2(
            Stage.GOSSIP, //
            GossipDigestAck2.serializer, //
            new GossipDigestAck2VerbHandler()),

    GOSSIP_SHUTDOWN(
            Stage.GOSSIP, //
            null, //
            new GossipShutdownVerbHandler()),

    ECHO(
            Stage.GOSSIP, //
            EchoMessage.serializer, //
            new EchoVerbHandler()),

    // remember to add new verbs at the end, since we serialize by ordinal
    UNUSED_1,
    UNUSED_2,
    UNUSED_3;

    public final Stage stage;
    public final IVersionedSerializer<?> serializer;
    public final IVerbHandler<?> verbHandler;

    private Verb() {
        this(Stage.INTERNAL_RESPONSE, null, null);
    }

    private Verb(Stage stage, IVersionedSerializer<?> serializer, IVerbHandler<?> verbHandler) {
        this.stage = stage;
        this.serializer = serializer;
        this.verbHandler = verbHandler;
    }
}
