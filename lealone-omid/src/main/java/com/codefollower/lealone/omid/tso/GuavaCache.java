/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.codefollower.lealone.omid.tso;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class GuavaCache implements Cache, RemovalListener<Long, Long> {

    private com.google.common.cache.Cache<Long, Long> cache;
    private long removed;

    public GuavaCache(int size) {
        cache = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(size).initialCapacity(size).removalListener(this)
                .build();
    }

    @Override
    public long set(long key, long value) {
        cache.put(key, value);
        // cache.cleanUp();
        return removed;
    }

    @Override
    public long get(long key) {
        Long result = cache.getIfPresent(key);
        return result == null ? 0 : result;
    }

    @Override
    public void onRemoval(RemovalNotification<Long, Long> notification) {
        if (notification.getCause() == RemovalCause.REPLACED) {
            return;
        }
        //        LOG.warn("Removing " + notification);
        //        new Exception().printStackTrace();
        removed = Math.max(removed, notification.getValue());
    }

}
