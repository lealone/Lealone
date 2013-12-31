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
package com.codefollower.lealone.atomicdb.utils;

import java.lang.management.ManagementFactory;
import java.util.Set;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codefollower.lealone.atomicdb.cache.*;
import com.codefollower.lealone.atomicdb.concurrent.JMXEnabledThreadPoolExecutorMBean;
import com.codefollower.lealone.atomicdb.config.DatabaseDescriptor;
import com.codefollower.lealone.atomicdb.db.ColumnFamilyStore;
import com.codefollower.lealone.atomicdb.db.RowIndexEntry;
import com.codefollower.lealone.atomicdb.db.compaction.CompactionManager;
import com.codefollower.lealone.atomicdb.net.MessagingService;
import com.codefollower.lealone.atomicdb.service.CacheService;
import com.google.common.collect.Iterables;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StatusLogger
{
    private static final Logger logger = LoggerFactory.getLogger(StatusLogger.class);

    public static void log()
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();

        // everything from o.a.c.concurrent
        logger.info(String.format("%-25s%10s%10s%15s%10s%18s", "Pool Name", "Active", "Pending", "Completed", "Blocked", "All Time Blocked"));
        Set<ObjectName> request, internal;
        try
        {
            request = server.queryNames(new ObjectName("com.codefollower.lealone.atomicdb.request:type=*"), null);
            internal = server.queryNames(new ObjectName("com.codefollower.lealone.atomicdb.internal:type=*"), null);
        }
        catch (MalformedObjectNameException e)
        {
            throw new RuntimeException(e);
        }
        for (ObjectName objectName : Iterables.concat(request, internal))
        {
            String poolName = objectName.getKeyProperty("type");
            JMXEnabledThreadPoolExecutorMBean threadPoolProxy = JMX.newMBeanProxy(server, objectName, JMXEnabledThreadPoolExecutorMBean.class);
            logger.info(String.format("%-25s%10s%10s%15s%10s%18s",
                                      poolName,
                                      threadPoolProxy.getActiveCount(),
                                      threadPoolProxy.getPendingTasks(),
                                      threadPoolProxy.getCompletedTasks(),
                                      threadPoolProxy.getCurrentlyBlockedTasks(),
                                      threadPoolProxy.getTotalBlockedTasks()));
        }
        // one offs
        logger.info(String.format("%-25s%10s%10s",
                                  "CompactionManager", CompactionManager.instance.getActiveCompactions(), CompactionManager.instance.getPendingTasks()));
        int pendingCommands = 0;
        for (int n : MessagingService.instance().getCommandPendingTasks().values())
        {
            pendingCommands += n;
        }
        int pendingResponses = 0;
        for (int n : MessagingService.instance().getResponsePendingTasks().values())
        {
            pendingResponses += n;
        }
        logger.info(String.format("%-25s%10s%10s",
                                  "MessagingService", "n/a", pendingCommands + "/" + pendingResponses));

        // Global key/row cache information
        AutoSavingCache<KeyCacheKey, RowIndexEntry> keyCache = CacheService.instance.keyCache;
        AutoSavingCache<RowCacheKey, IRowCacheEntry> rowCache = CacheService.instance.rowCache;

        int keyCacheKeysToSave = DatabaseDescriptor.getKeyCacheKeysToSave();
        int rowCacheKeysToSave = DatabaseDescriptor.getRowCacheKeysToSave();

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "Cache Type", "Size", "Capacity", "KeysToSave"));
        logger.info(String.format("%-25s%10s%25s%25s",
                                  "KeyCache",
                                  keyCache.weightedSize(),
                                  keyCache.getCapacity(),
                                  keyCacheKeysToSave == Integer.MAX_VALUE ? "all" : keyCacheKeysToSave));

        logger.info(String.format("%-25s%10s%25s%25s",
                                  "RowCache",
                                  rowCache.weightedSize(),
                                  rowCache.getCapacity(),
                                  rowCacheKeysToSave == Integer.MAX_VALUE ? "all" : rowCacheKeysToSave));

        // per-CF stats
        logger.info(String.format("%-25s%20s", "ColumnFamily", "Memtable ops,data"));
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            logger.info(String.format("%-25s%20s",
                                      cfs.keyspace.getName() + "." + cfs.name,
                                      cfs.getMemtableColumnsCount() + "," + cfs.getMemtableDataSize()));
        }
    }
}
