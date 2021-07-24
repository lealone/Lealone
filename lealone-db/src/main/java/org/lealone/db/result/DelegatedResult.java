/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.result;

import org.lealone.db.value.Value;

public class DelegatedResult implements Result {
    protected Result result;

    @Override
    public void reset() {
        result.reset();
    }

    @Override
    public Value[] currentRow() {
        return result.currentRow();
    }

    @Override
    public boolean next() {
        return result.next();
    }

    @Override
    public int getRowId() {
        return result.getRowId();
    }

    @Override
    public int getVisibleColumnCount() {
        return result.getVisibleColumnCount();
    }

    @Override
    public int getRowCount() {
        return result.getRowCount();
    }

    @Override
    public boolean needToClose() {
        return result.needToClose();
    }

    @Override
    public void close() {
        result.close();
    }

    @Override
    public String getAlias(int i) {
        return result.getAlias(i);
    }

    @Override
    public String getSchemaName(int i) {
        return result.getSchemaName(i);
    }

    @Override
    public String getTableName(int i) {
        return result.getTableName(i);
    }

    @Override
    public String getColumnName(int i) {
        return result.getColumnName(i);
    }

    @Override
    public int getColumnType(int i) {
        return result.getColumnType(i);
    }

    @Override
    public long getColumnPrecision(int i) {
        return result.getColumnPrecision(i);
    }

    @Override
    public int getColumnScale(int i) {
        return result.getColumnScale(i);
    }

    @Override
    public int getDisplaySize(int i) {
        return result.getDisplaySize(i);
    }

    @Override
    public boolean isAutoIncrement(int i) {
        return result.isAutoIncrement(i);
    }

    @Override
    public int getNullable(int i) {
        return result.getNullable(i);
    }

    @Override
    public void setFetchSize(int fetchSize) {
        result.setFetchSize(fetchSize);
    }

    @Override
    public int getFetchSize() {
        return result.getFetchSize();
    }

}
