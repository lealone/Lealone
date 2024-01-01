/*
 * Copyright 2016 Ognyan Bankov
 * <p>
 * All rights reserved. Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lealone.plugins.mongo.bson.command.auth;

import java.util.UUID;

import com.lealone.db.auth.scram.ScramPasswordData;

/**
 * Provides server side processing of the SCRAM SASL authentication
 */
// 改编自github.com/ogrebgr/scram-sasl
public class ScramSaslProcessor {

    public static ScramSaslProcessor create(long connectionId, UserDataLoader userDataLoader,
            Sender sender, int mechanism) {
        return new ScramSaslProcessor(connectionId, userDataLoader, sender, "SHA-" + mechanism,
                "HmacSHA" + mechanism);
    }

    public static boolean isNullOrEmpty(String string) {
        return string == null || string.isEmpty();
    }

    private final long connectionId;
    private final UserDataLoader userDataLoader;
    private final Sender sender;

    private State state = State.INITIAL;

    private boolean success;
    private boolean aborted;
    private String userName;
    private ScramServerFunctionality scramServerFunctionality;

    /**
     * Creates new ScramSaslServerProcessor
     * @param connectionId ID of the client connection 
     * @param userDataLoader loader for user data
     * @param sender Sender used to send messages to the clients
     * @param digestName Digest to be used
     * @param hmacName HMAC to be used
     */
    public ScramSaslProcessor(long connectionId, UserDataLoader userDataLoader, Sender sender,
            String digestName, String hmacName) {

        this(connectionId, userDataLoader, sender, digestName, hmacName, UUID.randomUUID().toString());
    }

    /**
     * Creates new ScramSaslServerProcessor.
     * Intended to be used in unit test (with a predefined serverPartNonce in order to have repeatability)
     * @param connectionId ID of the client connection 
     * @param userDataLoader loader for user data
     * @param sender Sender used to send messages to the clients
     * @param digestName Digest to be used
     * @param hmacName HMAC to be used
     * @param serverPartNonce In its first message server sends a nonce 
     *        which contains the client nonce and server part nonce
     */
    public ScramSaslProcessor(long connectionId, UserDataLoader userDataLoader, Sender sender,
            String digestName, String hmacName, String serverPartNonce) {
        if (userDataLoader == null) {
            throw new NullPointerException("userDataLoader cannot be null");
        }
        if (sender == null) {
            throw new NullPointerException("sender cannot be null");
        }
        if (isNullOrEmpty(digestName)) {
            throw new NullPointerException("digestName cannot be null or empty");
        }
        if (isNullOrEmpty(hmacName)) {
            throw new NullPointerException("hmacName cannot be null or empty");
        }
        if (isNullOrEmpty(serverPartNonce)) {
            throw new NullPointerException("serverPartNonce cannot be null or empty");
        }
        scramServerFunctionality = new ScramServerFunctionality(digestName, hmacName, serverPartNonce);

        this.connectionId = connectionId;
        this.userDataLoader = userDataLoader;
        this.sender = sender;
    }

    /**
     * Called when there is message from the client
     *
     * @param message Message 
     */
    public void onMessage(String message) {
        if (state != State.ENDED) {
            switch (state) {
            case INITIAL:
                if (handleClientFirst(message)) {
                    state = State.SERVER_FIRST_SENT;
                } else {
                    state = State.ENDED;
                }
                break;
            case SERVER_FIRST_SENT:
                state = State.ENDED;
                String msg = handleClientFinal(message);
                if (msg != null) {
                    sender.sendMessage(connectionId, msg, this);
                    success = true;
                }
                break;
            }
        }
    }

    /**
     * Called when {@link ScramPasswordData} is loaded by {@link UserDataLoader}
     *
     * @param data User data
     */
    public void onUserDataLoaded(ScramPasswordData data) {
        String serverFirstMessage = scramServerFunctionality.prepareFirstMessage(data);
        state = State.SERVER_FIRST_SENT;
        sender.sendMessage(connectionId, serverFirstMessage, this);
    }

    /**
     * Aborts the procedure
     */
    public void abort() {
        aborted = true;
        state = State.ENDED;
    }

    /**
     * Client connection's ID
     *
     * @return connection ID
     */
    public long getConnectionId() {
        return connectionId;
    }

    /**
     * Returns the authorized user name (you must ensure that procedure is completed 
     * and successful before calling this method)
     *
     * @return User name of authorized user
     */
    public String getAuthorizationID() {
        if (state == State.ENDED && success) {
            return userName;
        } else {
            throw new IllegalStateException("Don't call this method before the successful end");
        }
    }

    public Sender getSender() {
        return sender;
    }

    /**
     * Checks if authentication sequence has ended
     *
     * @return true if authentication has ended, false otherwise
     */
    public boolean isEnded() {
        return state == State.ENDED;
    }

    /**
     * Checks if the sequence has been aborted
     *
     * @return true if aborted, false otherwise
     */
    public boolean isAborted() {
        return aborted;
    }

    /**
     * Checks if authentication sequence has ended successfully (i.e. user is authenticated)
     *
     * @return true if authentication sequence has ended successfully, false otherwise
     */
    public boolean isSuccess() {
        if (state == State.ENDED && success) {
            return success;
        } else {
            throw new IllegalStateException("Don't call this method before the end");
        }
    }

    private boolean handleClientFirst(String message) {
        userName = scramServerFunctionality.handleClientFirstMessage(message);
        if (userName != null) {
            userDataLoader.loadUserData(userName, connectionId, this);
            return true;
        } else {
            return false;
        }
    }

    private String handleClientFinal(String message) {
        state = State.ENDED;
        String finalMessage = scramServerFunctionality.prepareFinalMessage(message);
        if (finalMessage != null) {
            success = true;
            state = State.ENDED;
            return finalMessage;
        } else {
            return null;
        }
    }

    private static enum State {
        INITIAL,
        SERVER_FIRST_SENT,
        ENDED
    }

    /**
     * Loads user data
     * Implementations will usually load the user data from a DB
     */
    public static interface UserDataLoader {
        /**
         * Called when user data is loaded
         *
         * @param userName     The user name
         * @param connectionId ID of the connection
         * @param processor    The client SCRAM processor
         */
        void loadUserData(String userName, long connectionId, ScramSaslProcessor processor);
    }

    /**
     * Provides functionality for sending message to the client
     */
    public static interface Sender {
        /**
         * Sends message to the client identified by connectionId
         *
         * @param connectionId ID of the client connection
         * @param msg          Message
         */
        void sendMessage(long connectionId, String msg, ScramSaslProcessor processor);
    }
}
