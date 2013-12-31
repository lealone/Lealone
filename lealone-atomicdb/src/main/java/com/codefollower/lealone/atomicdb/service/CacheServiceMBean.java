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
package com.codefollower.lealone.atomicdb.service;

import java.util.concurrent.ExecutionException;

public interface CacheServiceMBean
{
    public int getRowCacheSavePeriodInSeconds();
    public void setRowCacheSavePeriodInSeconds(int rcspis);

    public int getKeyCacheSavePeriodInSeconds();
    public void setKeyCacheSavePeriodInSeconds(int kcspis);

    public int getRowCacheKeysToSave();
    public void setRowCacheKeysToSave(int rckts);

    public int getKeyCacheKeysToSave();
    public void setKeyCacheKeysToSave(int kckts);

    /**
     * invalidate the key cache; for use after invalidating row cache
     */
    public void invalidateKeyCache();

    /**
     * invalidate the row cache; for use after bulk loading via BinaryMemtable
     */
    public void invalidateRowCache();

    public void setRowCacheCapacityInMB(long capacity);

    public void setKeyCacheCapacityInMB(long capacity);

    /**
     * save row and key caches
     *
     * @throws ExecutionException when attempting to retrieve the result of a task that aborted by throwing an exception
     * @throws InterruptedException when a thread is waiting, sleeping, or otherwise occupied, and the thread is interrupted, either before or during the activity.
     */
    public void saveCaches() throws ExecutionException, InterruptedException;

    //
    // remaining methods are provided for backwards compatibility; modern clients should use CacheMetrics instead
    //

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#hits
     */
    @Deprecated
    public long getKeyCacheHits();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#hits
     */
    @Deprecated
    public long getRowCacheHits();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#requests
     */
    @Deprecated
    public long getKeyCacheRequests();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#requests
     */
    @Deprecated
    public long getRowCacheRequests();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#hitRate
     */
    @Deprecated
    public double getKeyCacheRecentHitRate();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#hitRate
     */
    @Deprecated
    public double getRowCacheRecentHitRate();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getRowCacheCapacityInMB();
    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getRowCacheCapacityInBytes();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getKeyCacheCapacityInMB();
    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#capacity
     */
    @Deprecated
    public long getKeyCacheCapacityInBytes();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#size
     */
    @Deprecated
    public long getRowCacheSize();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#entries
     */
    @Deprecated
    public long getRowCacheEntries();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#size
     */
    @Deprecated
    public long getKeyCacheSize();

    /**
     * @see com.codefollower.lealone.atomicdb.metrics.CacheMetrics#entries
     */
    @Deprecated
    public long getKeyCacheEntries();
}
