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
import java.util.concurrent.atomic.AtomicInteger;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.db.Keyspace;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.lealone.cluster.gms.FailureDetector;
import org.lealone.cluster.locator.AbstractReplicationStrategy;
import org.lealone.cluster.locator.TokenMetaData;
import org.lealone.cluster.service.StorageService;
import org.lealone.cluster.streaming.StreamEvent;
import org.lealone.cluster.streaming.StreamEventHandler;
import org.lealone.cluster.streaming.StreamResultFuture;
import org.lealone.cluster.streaming.StreamState;
import org.lealone.cluster.utils.progress.ProgressEvent;
import org.lealone.cluster.utils.progress.ProgressEventNotifierSupport;
import org.lealone.cluster.utils.progress.ProgressEventType;
import org.lealone.db.Database;
import org.lealone.db.DatabaseEngine;
import org.lealone.db.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

public class BootStrapper extends ProgressEventNotifierSupport {
    private static final Logger logger = LoggerFactory.getLogger(BootStrapper.class);
    private static final boolean useStrictConsistency = Boolean.valueOf(Config.getProperty("consistent.rangemovement",
            "true"));

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

    public ListenableFuture<StreamState> bootstrap() {
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

        StreamResultFuture bootstrapStreamResult = streamer.fetchAsync();
        bootstrapStreamResult.addEventListener(new StreamEventHandler() {
            private final AtomicInteger receivedFiles = new AtomicInteger();
            private final AtomicInteger totalFilesToReceive = new AtomicInteger();

            @Override
            public void handleStreamEvent(StreamEvent event) {
                switch (event.eventType) {
                case STREAM_PREPARED:
                    StreamEvent.SessionPreparedEvent prepared = (StreamEvent.SessionPreparedEvent) event;
                    int currentTotal = totalFilesToReceive.addAndGet((int) prepared.session.getTotalFilesToReceive());
                    ProgressEvent prepareProgress = new ProgressEvent(ProgressEventType.PROGRESS, receivedFiles.get(),
                            currentTotal, "prepare with " + prepared.session.peer + " complete");
                    fireProgressEvent("bootstrap", prepareProgress);
                    break;

                case FILE_PROGRESS:
                    StreamEvent.ProgressEvent progress = (StreamEvent.ProgressEvent) event;
                    if (progress.progress.isCompleted()) {
                        int received = receivedFiles.incrementAndGet();
                        ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.PROGRESS, received,
                                totalFilesToReceive.get(), "received file " + progress.progress.fileName);
                        fireProgressEvent("bootstrap", currentProgress);
                    }
                    break;

                case STREAM_COMPLETE:
                    StreamEvent.SessionCompleteEvent completeEvent = (StreamEvent.SessionCompleteEvent) event;
                    ProgressEvent completeProgress = new ProgressEvent(ProgressEventType.PROGRESS, receivedFiles.get(),
                            totalFilesToReceive.get(), "session with " + completeEvent.peer + " complete");
                    fireProgressEvent("bootstrap", completeProgress);
                    break;
                }
            }

            @Override
            public void onSuccess(StreamState streamState) {
                ProgressEventType type;
                String message;

                if (streamState.hasFailedSession()) {
                    type = ProgressEventType.ERROR;
                    message = "Some bootstrap stream failed";
                } else {
                    type = ProgressEventType.SUCCESS;
                    message = "Bootstrap streaming success";
                }
                ProgressEvent currentProgress = new ProgressEvent(type, receivedFiles.get(), totalFilesToReceive.get(),
                        message);
                fireProgressEvent("bootstrap", currentProgress);
            }

            @Override
            public void onFailure(Throwable throwable) {
                ProgressEvent currentProgress = new ProgressEvent(ProgressEventType.ERROR, receivedFiles.get(),
                        totalFilesToReceive.get(), throwable.getMessage());
                fireProgressEvent("bootstrap", currentProgress);
            }
        });
        return bootstrapStreamResult;
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
