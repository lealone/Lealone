/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.lealone.omid;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * This class contains functionality that is useful for the Omid tests.
 * 
 */
public class TestUtils {
    private static final Log LOG = LogFactory.getLog(TestUtils.class);

    public static void waitForSocketListening(String host, int port, int sleepTimeMillis) throws UnknownHostException,
            IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                Thread.sleep(sleepTimeMillis);
                continue;
            } finally {
                if (sock != null) {
                    sock.close();
                }
            }
            LOG.info("Host " + host + ":" + port + " is up");
            break;
        }
    }

    public static void waitForSocketNotListening(String host, int port, int sleepTimeMillis) throws UnknownHostException,
            IOException, InterruptedException {
        while (true) {
            Socket sock = null;
            try {
                sock = new Socket(host, port);
            } catch (IOException e) {
                // ignore as this is expected
                break;
            } finally {
                if (sock != null) {
                    sock.close();
                }
            }
            Thread.sleep(sleepTimeMillis);
            LOG.info("Host " + host + ":" + port + " is up");
        }
    }

}
