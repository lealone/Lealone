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
package org.lealone.hbase.engine;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.lealone.hbase.zookeeper.RegionLocationCacheTracker;
import org.lealone.hbase.zookeeper.ZooKeeperAdmin;

public class RegionLocationCacheObserver extends BaseRegionObserver {
    private static RegionLocationCacheTracker tracker;
    private static ZooKeeperWatcher watcher;
    private static final AtomicLong count = new AtomicLong(1);

    public RegionLocationCacheObserver() {
        try {
            if (watcher == null) {
                synchronized (RegionLocationCacheObserver.class) {
                    if (watcher == null) {
                        watcher = ZooKeeperAdmin.getZooKeeperWatcher();
                        tracker = new RegionLocationCacheTracker(watcher);
                        tracker.start();
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preClose(ObserverContext<RegionCoprocessorEnvironment> c, boolean abortRequested) {
        setData(c);
    }

    @Override
    public void postSplit(ObserverContext<RegionCoprocessorEnvironment> c, HRegion l, HRegion r) throws IOException {
        setData(c);
    }

    private void setData(ObserverContext<RegionCoprocessorEnvironment> c) {
        String data = c.getEnvironment().getRegionServerServices().getServerName() + "_" + count.getAndIncrement();
        try {
            ZKUtil.setData(watcher, ZooKeeperAdmin.REGION_LOCATION_CACHE_NODE, Bytes.toBytes(data));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
