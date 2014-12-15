package org.lealone.cluster.service;

import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LealoneDaemon {
    private static final Logger logger = LoggerFactory.getLogger(LealoneDaemon.class);

    public static void main(String[] args) {
        // log warnings for different kinds of sub-optimal JVMs.  tldr use 64-bit Oracle >= 1.6u32
        if (!DatabaseDescriptor.hasLargeAddressSpace())
            logger.info("32bit JVM detected.  It is recommended to run lealone on a 64bit JVM for better performance.");

        try {
            StorageService.instance.initServer();
        } catch (ConfigurationException e) {
            System.err.println(e.getMessage() + //
                    "\nFatal configuration error; unable to start server.  See log for stacktrace.");
        }
    }

}
