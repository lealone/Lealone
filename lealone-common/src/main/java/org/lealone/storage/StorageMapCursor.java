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
package org.lealone.storage;

import java.util.Iterator;

import org.lealone.common.util.DataUtils;

public interface StorageMapCursor<K, V> extends Iterator<K> {

    /**
     * Get the last read key if there was one.
     *
     * @return the key or null
     */
    K getKey();

    /**
     * Get the last read value if there was one.
     *
     * @return the value or null
     */
    V getValue();

    @Override
    default void remove() {
        throw DataUtils.newUnsupportedOperationException("Removing is not supported");
    }
}
