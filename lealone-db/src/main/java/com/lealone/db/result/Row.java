/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.lealone.db.result;

import com.lealone.db.value.Value;
import com.lealone.db.value.ValueLong;
import com.lealone.storage.page.IPage;
import com.lealone.transaction.ITransactionalValue;

/**
 * Represents a row in a table.
 */
public class Row extends SimpleRow {

    public static final int MEMORY_CALCULATE = -1;

    private ITransactionalValue tv;
    private IPage page;

    public Row(Value[] data, int memory) {
        super(data);
        this.memory = memory;
    }

    public Value[] getValueList() {
        return data;
    }

    public ITransactionalValue getTValue() {
        return tv;
    }

    public void setTValue(ITransactionalValue tv) {
        this.tv = tv;
    }

    @Override
    public Value getValue(int i) {
        return i == -1 ? ValueLong.get(key) : data[i];
    }

    @Override
    public void setValue(int i, Value v) {
        if (i == -1) {
            key = v.getLong();
        } else {
            data[i] = v;
        }
    }

    public IPage getPage() {
        return page;
    }

    public void setPage(IPage page) {
        this.page = page;
    }
}
