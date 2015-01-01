package org.lealone.cluster.service;

import java.util.ArrayList;

import org.lealone.cluster.config.Config;
import org.lealone.cluster.config.DatabaseDescriptor;
import org.lealone.cluster.router.P2PRouter;
import org.lealone.engine.Session;
import org.lealone.server.TcpServer;
import org.lealone.transaction.TransactionalRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LealoneDaemon {
    private static final Logger logger = LoggerFactory.getLogger(LealoneDaemon.class);

    public static void main(String[] args) {
        // log warnings for different kinds of sub-optimal JVMs.  tldr use 64-bit Oracle >= 1.6u32
        if (!DatabaseDescriptor.hasLargeAddressSpace())
            logger.info("32bit JVM detected.  It is recommended to run lealone on a 64bit JVM for better performance.");

        try {
            if (DatabaseDescriptor.loadConfig().isClusterMode()) {
                Session.setRouter(new TransactionalRouter(P2PRouter.getInstance()));
                StorageService.instance.initServer();
            }

            startTcpServer();
        } catch (Exception e) {
            System.err.println(e.getMessage() + //
                    "\nFatal configuration error; unable to start server.  See log for stacktrace.");
        }
    }

    private static void startTcpServer() throws Exception {
        Config config = DatabaseDescriptor.loadConfig();
        ArrayList<String> list = new ArrayList<>();
        list.add("-baseDir");
        list.add(config.base_dir);
        list.add("-tcpListenAddress");
        list.add(config.listen_address);
        if (config.tcp_port > 0) {
            list.add("-tcpPort");
            list.add(Integer.toString(config.tcp_port));
        }

        if (config.tcp_allow_others)
            list.add("-tcpAllowOthers");

        if (config.tcp_daemon)
            list.add("-tcpDaemon");

        TcpServer server = new TcpServer();

        server.init(list.toArray(new String[list.size()]));
        server.start();
        logger.info("Lealone TcpServer started, listening address: {}, port: {}", config.listen_address, server.getPort());
        server.listen();
    }

}
