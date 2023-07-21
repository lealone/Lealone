/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

import org.lealone.db.async.AsyncHandler;

public interface DirtyPageHandler<E> extends AsyncHandler<E> {
    void addDirtyPage(IPage page);
}
