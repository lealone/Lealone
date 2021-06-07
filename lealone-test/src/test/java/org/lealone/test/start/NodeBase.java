/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.start;

import java.util.concurrent.CountDownLatch;

import org.lealone.common.exceptions.ConfigException;
import org.lealone.main.Lealone;
import org.lealone.p2p.config.Config;
import org.lealone.p2p.config.YamlConfigLoader;

public class NodeBase extends YamlConfigLoader {

    public static void run(Class<?> loader, String[] args) {
        init(loader);
        Lealone.main(args);
    }

    public static void run(Class<?> loader, String[] args, CountDownLatch latch) {
        init(loader);
        Lealone.run(args, false, latch);
    }

    private static void init(Class<?> loader) {
        System.setProperty("lealone.config.loader", loader.getName());
        System.setProperty("lealone.config", "lealone-test.yaml");

        System.setProperty("lealone.load.persisted.node.info", "false"); // 不从nodes表加载ring状态信息

        // 见P2pServer.getRingDelay()
        // 默认30秒，等太久了
        System.setProperty("lealone.ring.delay.ms", "5000");

        // System.setProperty("DATABASE_TO_UPPER", "false");
        System.setProperty("lealone.lobInDatabase", "false");
        System.setProperty("lealone.lobClientMaxSizeMemory", "1024");
        // System.setProperty("lealone.check2", "true");
    }

    protected String listen_address;
    protected String dir;
    protected String nodeBaseDirPrefix;
    protected String node_snitch;

    // 在org.lealone.common.util.Utils.construct(String, String)中必须使用无参数的构造函数
    public NodeBase() {
    }

    @Override
    public void applyConfig(Config config) throws ConfigException {
        config.base_dir = config.base_dir + "/" + nodeBaseDirPrefix;
        if (dir != null) {
            config.base_dir = config.base_dir + dir;
        }

        if (listen_address != null)
            config.listen_address = listen_address;

        if (node_snitch != null)
            config.cluster_config.node_snitch = node_snitch;

        System.setProperty("java.io.tmpdir", "./" + config.base_dir + "/tmp");
        System.setProperty("lealone.base.dir", "./" + config.base_dir);

        super.applyConfig(config);
    }
}
