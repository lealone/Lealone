/*
 * Copyright 2011 The Apache Software Foundation
 *
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
package org.lealone.hbase.zookeeper;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.lealone.hbase.util.HBaseUtils;

public class RegionLocationCacheTracker extends ZooKeeperListener {
    private static final Log log = LogFactory.getLog(RegionLocationCacheTracker.class);

    public RegionLocationCacheTracker(ZooKeeperWatcher watcher) {
        super(watcher);
    }

    public void start() {
        watcher.registerListener(this);
        getDataAndWatch(); //必须watch，否则收不到zk的通知
    }

    @Override
    public void nodeDataChanged(String path) {
        if (path.equals(ZooKeeperAdmin.REGION_LOCATION_CACHE_NODE)) {
            try {
                getDataAndWatch();
                HBaseUtils.getConnection().clearRegionCache();
            } catch (IOException e) {
                log.warn("cannot clear region cache", e);
            }
        }
    }

    @Override
    public void nodeChildrenChanged(String path) {
        nodeDataChanged(path);
    }

    private void getDataAndWatch() {
        try {
            ZKUtil.getDataAndWatch(watcher, ZooKeeperAdmin.REGION_LOCATION_CACHE_NODE);
        } catch (Exception e) {
            log.warn("cannot watch zk node: " + ZooKeeperAdmin.REGION_LOCATION_CACHE_NODE, e);
        }
    }
}