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
