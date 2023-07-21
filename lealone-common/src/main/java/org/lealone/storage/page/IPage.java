/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.storage.page;

public interface IPage {
    void markDirtyBottomUp();

    boolean addLockOnwer(Object onwer);

    void removeLockOnwer(Object onwer);
}
