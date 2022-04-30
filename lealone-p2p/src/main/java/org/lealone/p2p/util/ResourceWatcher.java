/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.util;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.lealone.common.concurrent.ScheduledExecutors;
import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.JVMStabilityInspector;

public class ResourceWatcher {

    public static void watch(String resource, Runnable callback, int period) {
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(new WatchedResource(resource, callback), period,
                period, TimeUnit.MILLISECONDS);
    }

    public static class WatchedResource implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(WatchedResource.class);
        private final String resource;
        private final Runnable callback;
        private long lastLoaded;

        public WatchedResource(String resource, Runnable callback) {
            this.resource = resource;
            this.callback = callback;
            lastLoaded = 0;
        }

        @Override
        public void run() {
            try {
                String filename = P2pUtils.resourceToFile(resource);
                long lastModified = new File(filename).lastModified();
                if (lastModified > lastLoaded) {
                    callback.run();
                    lastLoaded = lastModified;
                }
            } catch (Throwable t) {
                JVMStabilityInspector.inspectThrowable(t);
                logger.error(String.format("Timed run of %s failed.", callback.getClass()), t);
            }
        }
    }
}
