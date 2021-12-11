/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.index;

import org.lealone.db.result.Row;

public class EmptyCursor implements Cursor {

    public static final EmptyCursor INSTANCE = new EmptyCursor();

    @Override
    public Row get() {
        return null;
    }

    @Override
    public boolean next() {
        return false;
    }
}
