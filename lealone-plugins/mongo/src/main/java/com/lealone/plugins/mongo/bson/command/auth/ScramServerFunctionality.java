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

import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.lealone.db.auth.scram.ScramPasswordData;
import com.lealone.db.auth.scram.ScramPasswordHash;

/**
 * Provides building blocks for creating SCRAM authentication server
 */
// 改编自github.com/ogrebgr/scram-sasl
public class ScramServerFunctionality {

    private static final Pattern CLIENT_FIRST_MESSAGE = Pattern
            .compile("^(([pny])=?([^,]*),([^,]*),)(m?=?[^,]*,?n=([^,]*),r=([^,]*),?.*)$");
    private static final Pattern CLIENT_FINAL_MESSAGE = Pattern.compile("(c=([^,]*),r=([^,]*)),p=(.*)$");

    private final String digestName;
    private final String hmacName;
    private final String serverPartNonce;

    private boolean isSuccessful;
    private State state = State.INITIAL;
    private String clientFirstMessageBare;
    private String nonce;
    private String serverFirstMessage;
    private ScramPasswordData userData;

    /**
     /**
     * Creates new ScramServerFunctionality 
     * @param digestName Digest to be used
     * @param hmacName HMAC to be used
     * @param serverPartNonce Server's part of the nonce
     */
    public ScramServerFunctionality(String digestName, String hmacName, String serverPartNonce) {
        if (ScramSaslProcessor.isNullOrEmpty(digestName)) {
            throw new NullPointerException("digestName cannot be null or empty");
        }
        if (ScramSaslProcessor.isNullOrEmpty(hmacName)) {
            throw new NullPointerException("hmacName cannot be null or empty");
        }
        if (ScramSaslProcessor.isNullOrEmpty(serverPartNonce)) {
            throw new NullPointerException("serverPartNonce cannot be null or empty");
        }

        this.digestName = digestName;
        this.hmacName = hmacName;
        this.serverPartNonce = serverPartNonce;
    }

    /**
     * Handles client's first message
     * @param message Client's first message
     * @return userName extracted from the client message
     */
    public String handleClientFirstMessage(String message) {
        Matcher m = CLIENT_FIRST_MESSAGE.matcher(message);
        if (!m.matches()) {
            return null;
        }

        clientFirstMessageBare = m.group(5);
        String userName = m.group(6);
        String clientNonce = m.group(7);
        nonce = clientNonce + serverPartNonce;

        state = State.FIRST_CLIENT_MESSAGE_HANDLED;

        return userName;
    }

    /**
     * Prepares server's final message
     * @param clientFinalMessage Client's final message
     * @return Server's final message
     * @throws ScramException if there is an error processing clients message
     */
    public String prepareFirstMessage(ScramPasswordData userData) {
        this.userData = userData;
        state = State.PREPARED_FIRST;
        serverFirstMessage = String.format("r=%s,s=%s,i=%d", nonce,
                Base64.getEncoder().encodeToString(userData.salt), userData.iterations);
        return serverFirstMessage;
    }

    /**
     * Prepares server's final message
     * @param clientFinalMessage Client's final message
     * @return Server's final message 
     */
    public String prepareFinalMessage(String clientFinalMessage) {
        Matcher m = CLIENT_FINAL_MESSAGE.matcher(clientFinalMessage);
        if (!m.matches()) {
            state = State.ENDED;
            return null;
        }

        String clientFinalMessageWithoutProof = m.group(1);
        String clientNonce = m.group(3);
        String proof = m.group(4);

        if (!nonce.equals(clientNonce)) {
            state = State.ENDED;
            return null;
        }

        String authMessage = clientFirstMessageBare + "," + serverFirstMessage + ","
                + clientFinalMessageWithoutProof;

        byte[] storedKeyArr = userData.storedKey;

        try {
            byte[] clientSignature = ScramPasswordHash.computeHmac(storedKeyArr, hmacName, authMessage);
            byte[] serverSignature = ScramPasswordHash.computeHmac(userData.serverKey, hmacName,
                    authMessage);
            byte[] clientKey = clientSignature.clone();
            byte[] decodedProof = Base64.getDecoder().decode(proof);
            for (int i = 0; i < clientKey.length; i++) {
                clientKey[i] ^= decodedProof[i];
            }

            byte[] resultKey = MessageDigest.getInstance(digestName).digest(clientKey);
            if (!Arrays.equals(storedKeyArr, resultKey)) {
                return null;
            }

            isSuccessful = true;
            state = State.ENDED;
            return "v=" + Base64.getEncoder().encodeToString(serverSignature);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            state = State.ENDED;
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if authentication is completed, either successfully or not.
     * Authentication is completed if {@link #getState()} returns ENDED.
     * @return true if authentication has ended
     */
    public boolean isSuccessful() {
        if (state == State.ENDED) {
            return isSuccessful;
        } else {
            throw new IllegalStateException(
                    "You cannot call this method before authentication is ended. "
                            + "Use isEnded() to check that");
        }
    }

    /**
     * Checks if authentication is completed, either successfully or not.
     * Authentication is completed if {@link #getState()} returns ENDED.
     * @return true if authentication has ended
     */
    public boolean isEnded() {
        return state == State.ENDED;
    }

    /**
     * State of the authentication procedure
     */
    private static enum State {
        /**
         * Initial state
         */
        INITIAL,
        /**
         * First client message is handled (user name is extracted)
         */
        FIRST_CLIENT_MESSAGE_HANDLED,
        /**
         * First server message is prepared
         */
        PREPARED_FIRST,
        /**
         * Authentication is completes, either successfully or not
         */
        ENDED
    }
}
