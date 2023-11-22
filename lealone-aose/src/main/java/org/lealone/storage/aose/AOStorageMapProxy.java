/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.aose;

import org.lealone.db.scheduler.Scheduler;
import org.lealone.storage.StorageMapProxy;
import org.lealone.storage.aose.btree.BTreeMap;

public class AOStorageMapProxy<K, V> extends StorageMapProxy<K, V> {

    // private final BTreeMap<K, V> map;

    public AOStorageMapProxy(BTreeMap<K, V> map, Scheduler scheduler) {
        super(map, scheduler);
    }
}
