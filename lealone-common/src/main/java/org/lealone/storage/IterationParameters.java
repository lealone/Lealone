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

import java.util.List;

public class IterationParameters<K> {

    public K from;
    public K to;
    public List<PageKey> pageKeys;
    public int[] columnIndexes;
    public boolean allColumns;

    public <K2> IterationParameters<K2> copy(K2 from, K2 to) {
        IterationParameters<K2> p = new IterationParameters<>();
        p.from = from;
        p.to = to;
        p.pageKeys = pageKeys;
        p.columnIndexes = columnIndexes;
        return p;
    }

    public static <K> IterationParameters<K> create(K from) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, K to) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.to = to;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, List<PageKey> pageKeys) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.pageKeys = pageKeys;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, int columnIndex) {
        return create(from, new int[] { columnIndex });
    }

    public static <K> IterationParameters<K> create(K from, int[] columnIndexes) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.columnIndexes = columnIndexes;
        return p;
    }

    public static <K> IterationParameters<K> create(K from, K to, List<PageKey> pageKeys, int[] columnIndexes) {
        IterationParameters<K> p = new IterationParameters<>();
        p.from = from;
        p.to = to;
        p.pageKeys = pageKeys;
        p.columnIndexes = columnIndexes;
        return p;
    }
}
