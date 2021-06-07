/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.server;

import org.lealone.net.NetNode;

/**
 * Encapsulates the callback information.
 * The ability to set the message is useful in cases for when a hint needs 
 * to be written due to a timeout in the response from a replica.
 */
public class CallbackInfo {

    public final NetNode target;
    public final IAsyncCallback<?> callback;
    public final boolean failureCallback;

    public CallbackInfo(NetNode target, IAsyncCallback<?> callback) {
        this(target, callback, false);
    }

    /**
     * Create CallbackInfo without sent message
     *
     * @param target target to send message
     * @param callback
     */
    public CallbackInfo(NetNode target, IAsyncCallback<?> callback, boolean failureCallback) {
        this.target = target;
        this.callback = callback;
        this.failureCallback = failureCallback;
    }

    public boolean isFailureCallback() {
        return failureCallback;
    }

    @Override
    public String toString() {
        return "CallbackInfo(" + "target=" + target + ", callback=" + callback + ", failureCallback=" + failureCallback
                + ')';
    }
}
