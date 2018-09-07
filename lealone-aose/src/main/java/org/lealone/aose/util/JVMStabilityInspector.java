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
package org.lealone.aose.util;

import java.io.FileNotFoundException;
import java.net.SocketException;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.ShutdownHookUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Responsible for deciding whether to kill the JVM if it gets in an "unstable" state (think OOM).
 */
public final class JVMStabilityInspector {

    private static final Logger logger = LoggerFactory.getLogger(JVMStabilityInspector.class);
    private static final Killer killer = new Killer();

    private JVMStabilityInspector() {
    }

    /**
     * Certain Throwables and Exceptions represent "Die" conditions for the server.
     * @param t
     *      The Throwable to check for server-stop conditions
     */
    public static void inspectThrowable(Throwable t) {
        boolean isUnstable = false;
        if (t instanceof OutOfMemoryError)
            isUnstable = true;
        // Check for file handle exhaustion
        else if (t instanceof FileNotFoundException || t instanceof SocketException)
            if (t.getMessage().contains("Too many open files"))
                isUnstable = true;

        if (isUnstable)
            killer.killCurrentJVM(t);
    }

    @VisibleForTesting
    public static class Killer {
        /**
        * Certain situations represent "Die" conditions for the server, and if so, 
        * the reason is logged and the current JVM is killed.
        *
        * @param t
        *      The Throwable to log before killing the current JVM
        */
        protected void killCurrentJVM(Throwable t) {
            t.printStackTrace(System.err);
            logger.error("JVM state determined to be unstable.  Exiting forcefully due to:", t);
            ShutdownHookUtils.removeAllShutdownHooks();
            System.exit(100);
        }
    }
}
