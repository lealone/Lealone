package org.h2.expression;

import org.h2.value.Value;

public class RowKeyConditionInfo {
    private String rowKeyName;

    private int compareTypeStart = -1;
    private Value compareValueStart;
    private int compareTypeStop = -1;
    private Value compareValueStop;

    public RowKeyConditionInfo(String rowKeyName) {
        this.rowKeyName = rowKeyName;
    }

    public String getRowKeyName() {
        return rowKeyName;
    }

    public void setRowKeyName(String rowKeyName) {
        this.rowKeyName = rowKeyName;
    }

    public int getCompareTypeStart() {
        return compareTypeStart;
    }

    public void setCompareTypeStart(int compareTypeStart) {
        this.compareTypeStart = compareTypeStart;
    }

    public Value getCompareValueStart() {
        return compareValueStart;
    }

    public void setCompareValueStart(Value compareValueStart) {
        this.compareValueStart = compareValueStart;
    }

    public int getCompareTypeStop() {
        return compareTypeStop;
    }

    public void setCompareTypeStop(int compareTypeStop) {
        this.compareTypeStop = compareTypeStop;
    }

    public Value getCompareValueStop() {
        return compareValueStop;
    }

    public void setCompareValueStop(Value compareValueStop) {
        this.compareValueStop = compareValueStop;
    }

}
