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
package org.lealone.cluster.dht;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.Keyspace;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.gms.FailureDetector;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.locator.TokenMetaData;
import org.lealone.cluster.service.StorageService;
import org.lealone.dbobject.Schema;
import org.lealone.engine.Database;
import org.lealone.engine.DatabaseEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BootStrapper {
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);
    private static final boolean useStrictConsistency = Boolean.valueOf(System.getProperty(
            "lealone.consistent.rangemovement", "true"));

    /* endpoint that needs to be bootstrapped */
    protected final InetAddress address;
    /* token of the node being bootstrapped. */
    protected final Collection<Token> tokens;
    protected final TokenMetaData tokenMetaData;

    public BootStrapper(InetAddress address, Collection<Token> tokens, TokenMetaData tmd) {
        assert address != null;
        assert tokens != null && !tokens.isEmpty();

        this.address = address;
        this.tokens = tokens;
        tokenMetaData = tmd;
    }

    public void bootstrap() {
        if (logger.isDebugEnabled())
            logger.debug("Beginning bootstrap process");
        RangeStreamer streamer = new RangeStreamer(tokenMetaData, tokens, address, "Bootstrap", useStrictConsistency,
                DatabaseDescriptor.getEndpointSnitch());
        streamer.addSourceFilter(new RangeStreamer.FailureDetectorSourceFilter(FailureDetector.instance));

        for (Database db : DatabaseEngine.getDatabases()) {
            if (!db.isPersistent())
                continue;

            for (Schema schema : db.getAllSchemas()) {
                AbstractReplicationStrategy strategy = Keyspace.getReplicationStrategy(schema);
                streamer.addRanges(schema, strategy.getPendingAddressRanges(tokenMetaData, tokens, address));
            }
        }

        StorageService.instance.finishBootstrapping();
    }

    /**
     * if num_tokens == 1, pick a token to assume half the load of the most-loaded node.
     * else choose num_tokens tokens at random
     */
    public static Collection<Token> getBootstrapTokens(final TokenMetaData metadata) throws ConfigurationException {
        int numTokens = DatabaseDescriptor.getNumTokens();
        if (numTokens < 1)
            throw new ConfigurationException("num_tokens must be >= 1");

        if (numTokens == 1)
            logger.warn("Picking random token for a single vnode. You should probably add more vnodes.");

        return getRandomTokens(metadata, numTokens);
    }

    public static Collection<Token> getRandomTokens(TokenMetaData metadata, int numTokens) {
        Set<Token> tokens = new HashSet<Token>(numTokens);
        while (tokens.size() < numTokens) {
            Token token = StorageService.getPartitioner().getRandomToken();
            if (metadata.getEndpoint(token) == null)
                tokens.add(token);
        }
        return tokens;
    }
}
