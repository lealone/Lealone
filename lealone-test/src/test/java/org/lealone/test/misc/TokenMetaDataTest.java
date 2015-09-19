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
package org.lealone.test.misc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.Keyspace;
import org.lealone.cluster.dht.Murmur3Partitioner.LongToken;
import org.lealone.cluster.dht.Range;
import org.lealone.cluster.dht.RangeStreamer;
import org.lealone.cluster.dht.Token;
import org.lealone.cluster.gms.Gossiper;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.locator.IEndpointSnitch;
import org.lealone.cluster.locator.SimpleStrategy;
import org.lealone.cluster.locator.TokenMetaData;
import org.lealone.dbobject.Schema;
import org.lealone.test.dbobject.DbObjectTestBase;

public class TokenMetaDataTest extends DbObjectTestBase {

    public static void main(String[] args) throws UnknownHostException {
        new TokenMetaDataTest().run();
    }

    TokenMetaData tokenMetaData;

    TokenMetaDataTest() {
        System.setProperty("lealone.config", "lealone-onedc.yaml");
        DatabaseDescriptor.loadConfig();
        tokenMetaData = new TokenMetaData();
    }

    public void add(String endpointStr, long... tokens) throws UnknownHostException {
        InetAddress endpoint = InetAddress.getByName(endpointStr);
        UUID uuid = UUID.randomUUID();
        tokenMetaData.updateNormalTokens(createTokens(tokens), endpoint);
        tokenMetaData.updateHostId(uuid, endpoint);

        Gossiper.instance.initializeNodeUnsafe(endpoint, uuid, 1);
    }

    ArrayList<Token> createTokens(long... tokens) {
        ArrayList<Token> tokensList = new ArrayList<>(tokens.length);
        for (long t : tokens) {
            tokensList.add(new LongToken(t));
        }
        return tokensList;
    }

    public void run() throws UnknownHostException {

        add("127.0.0.1", 10, 50);
        add("127.0.0.2", 20, 60);
        add("127.0.0.3", 30, 70);
        add("127.0.0.4", 40, 80);

        InetAddress endpoint = InetAddress.getByName("127.0.0.2");
        tokenMetaData.addLeavingEndpoint(endpoint);

        endpoint = InetAddress.getByName("127.0.0.4");
        Token token = new LongToken(35L);
        tokenMetaData.addMovingEndpoint(token, endpoint);

        endpoint = InetAddress.getByName("127.0.0.5");
        tokenMetaData.addBootstrapTokens(createTokens(-10, 90), endpoint);
        tokenMetaData.updateHostId(UUID.randomUUID(), endpoint);

        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        String keyspaceName = "test";
        HashMap<String, String> configOptions = new HashMap<>();
        configOptions.put("replication_factor", "3");
        AbstractReplicationStrategy strategy = new SimpleStrategy(keyspaceName, tokenMetaData, snitch, configOptions);
        tokenMetaData.calculatePendingRanges(strategy, keyspaceName);

        ArrayList<Token> tokens = createTokens(15, 90);
        endpoint = InetAddress.getByName("127.0.0.6");
        // getPendingAddressRanges并不会考虑leavingEndpoints和movingEndpoints之类的情况
        Collection<Range<Token>> ranges = strategy.getPendingAddressRanges(tokenMetaData, tokens, endpoint);
        System.out.println(ranges);

        String userName = "sa1";
        executeUpdate("CREATE USER IF NOT EXISTS " + userName + " PASSWORD 'abc' ADMIN");
        int id = db.allocateObjectId();
        String schemaName = "test";
        Schema schema = new Schema(db, id, schemaName, db.getUser(userName), false);
        configOptions.put("class", SimpleStrategy.class.getName());
        schema.setReplicationProperties(configOptions);

        strategy = Keyspace.getReplicationStrategy(schema);
        ranges = strategy.getPendingAddressRanges(tokenMetaData, tokens, endpoint);

        RangeStreamer rangeStreamer;

        rangeStreamer = new RangeStreamer(tokenMetaData, tokens, endpoint, "testRangeStreamer", true, snitch);
        rangeStreamer.addRanges(schema, ranges);

        rangeStreamer = new RangeStreamer(tokenMetaData, tokens, endpoint, "testRangeStreamer", false, snitch);
        rangeStreamer.addRanges(schema, ranges);
    }
}
