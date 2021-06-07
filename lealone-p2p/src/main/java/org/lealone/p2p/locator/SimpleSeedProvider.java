/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.p2p.locator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.lealone.common.logging.Logger;
import org.lealone.common.logging.LoggerFactory;
import org.lealone.common.util.StringUtils;
import org.lealone.net.NetNode;

public class SimpleSeedProvider implements SeedProvider {

    private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);

    private final List<NetNode> seeds;

    public SimpleSeedProvider(Map<String, String> parameters) {
        // 允许包含端口号
        String[] hosts = StringUtils.arraySplit(parameters.get("seeds"), ',');
        List<NetNode> seeds = new ArrayList<>(hosts.length);
        for (String host : hosts) {
            try {
                seeds.add(NetNode.createP2P(host));
            } catch (Exception ex) {
                // not fatal... DD will bark if there end up being zero seeds.
                logger.warn("Seed provider couldn't lookup host {}", host);
            }
        }
        this.seeds = Collections.unmodifiableList(seeds);
    }

    @Override
    public List<NetNode> getSeeds() {
        return seeds;
    }

}
