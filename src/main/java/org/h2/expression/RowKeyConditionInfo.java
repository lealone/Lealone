package org.h2.expression;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.h2.command.dml.Select;
import org.h2.table.HBaseTable;
import org.h2.util.HBaseUtils;
import org.h2.value.Value;

public class RowKeyConditionInfo {
    //private final HBaseTable table;
    private final Select select;
    private final String rowKeyName;
    private final byte[] tableName;

    private int compareTypeStart = -1;
    private Value compareValueStart;
    private int compareTypeStop = -1;
    private Value compareValueStop;

    private List<byte[]> startKeys;
    private byte[] startKey;
    private byte[] endKey;

    public RowKeyConditionInfo(HBaseTable table, Select select) {
        //this.table = table;
        this.select = select;
        this.rowKeyName = table.getRowKeyName();
        this.tableName = Bytes.toBytes(table.getName());
    }

    public String getRowKeyName() {
        return rowKeyName;
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

    public String[] getRowKeys() {
        String[] rowKeys = new String[2];
        if (getCompareValueStart() != null)
            rowKeys[0] = getCompareValueStart().getString();
        if (getCompareValueStop() != null)
            rowKeys[1] = getCompareValueStop().getString();
        return rowKeys;
    }

    public List<byte[]> getStartKeys() {
        if (startKeys == null) {
            String[] rowKeys = getRowKeys();
            if (rowKeys != null) {
                if (rowKeys.length >= 1 && rowKeys[0] != null)
                    startKey = Bytes.toBytes(rowKeys[0]);

                if (rowKeys.length >= 2 && rowKeys[1] != null)
                    endKey = Bytes.toBytes(rowKeys[1]);
            }

            if (startKey == null)
                startKey = HConstants.EMPTY_START_ROW;
            if (endKey == null)
                endKey = HConstants.EMPTY_END_ROW;
            try {
                startKeys = HBaseUtils.getStartKeysInRange(tableName, startKey, endKey);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return startKeys;
    }

    public String getRowKey() {
        String rowKey = null;

        if (getCompareTypeStart() != Comparison.EQUAL)
            throw new RuntimeException("rowKey compare type is not '='");
        if (getCompareValueStart() != null)
            rowKey = getCompareValueStart().getString();

        return rowKey;
    }

    public String[] getPlanSQLs() {
        getStartKeys();

        String[] sqls = null;
        if (startKeys != null) {
            boolean isDistributed = true;
            if (startKeys.size() == 1) {
                isDistributed = false;
            }

            int size = startKeys.size();
            sqls = new String[size];
            for (int i = 0; i < size; i++) {
                sqls[i] = select.getPlanSQL(startKeys.get(i), endKey, isDistributed);
            }
        }

        return sqls;
    }

    public int getStartKeysSize() {
        getStartKeys();
        if (startKeys == null)
            return 0;
        else
            return startKeys.size();
    }
}
