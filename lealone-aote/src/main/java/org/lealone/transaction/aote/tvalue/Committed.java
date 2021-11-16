/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.transaction.aote.tvalue;

import org.lealone.db.DataBuffer;

class Committed extends TValueBase {

    Committed(Object value) {
        super(value);
    }

    @Override
    public TransactionalValue getOldValue() {
        return null;
    }

    @Override
    public void setOldValue(TransactionalValue oldValue) {
    }

    @Override
    public void writeMeta(DataBuffer buff) {
        buff.putVarLong(0);
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder("Committed[ ");
        buff.append(value).append(" ]");
        return buff.toString();
    }
}
