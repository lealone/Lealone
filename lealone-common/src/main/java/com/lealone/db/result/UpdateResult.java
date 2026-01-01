/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.result;

public class UpdateResult extends DelegatedResult {

    private final int updateCount;

    public UpdateResult(int updateCount) {
        this.updateCount = updateCount;
    }

    @Override
    public boolean isUpdate() {
        return true;
    }

    @Override
    public int getUpdateCount() {
        return updateCount;
    }
}
