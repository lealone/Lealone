/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.transaction;

import com.lealone.storage.StorageMapCursor;

public interface TransactionMapCursor<K, V> extends StorageMapCursor<K, V> {

    ITransactionalValue getTValue();

}
