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
package com.codefollower.lealone.omid.tso;

public class LIRSCache implements Cache {
    CacheLongKeyLIRS<Long> cache;
    @SuppressWarnings("unused")
    private long removed;

    public LIRSCache(int size) {
        cache = new CacheLongKeyLIRS<Long>(size);
    }

    @Override
    public long set(long key, long value) {
        Long result = cache.put(key,Long.valueOf(value));
        return result == null ? 0 : result.longValue();
    }

    @Override
    public long get(long key) {
        Long result = cache.get(key);
        return result == null ? 0 : result.longValue();
    }

}
