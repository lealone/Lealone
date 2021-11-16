/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import org.lealone.db.DataBuffer;

class Removed extends TValueBase {
    private TransactionalValue oldValue;

    Removed(TransactionalValue oldValue) {
        super(oldValue.getValue());
        this.oldValue = oldValue;
    }

    @Override
    public TransactionalValue getOldValue() {
        return oldValue;
    }

    @Override
    public void setOldValue(TransactionalValue oldValue) {
        this.oldValue = oldValue;
    }

    @Override
    public void writeMeta(DataBuffer buff) {
        buff.putVarLong(0);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("Removed[ ");
        buff.append(value).append(" ]");
        return buff.toString();
    }
}
