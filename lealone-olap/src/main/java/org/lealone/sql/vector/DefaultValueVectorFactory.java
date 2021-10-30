/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import java.util.List;

import org.lealone.db.result.Row;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;

public class DefaultValueVectorFactory implements ValueVectorFactory {

    public static final DefaultValueVectorFactory INSTANCE = new DefaultValueVectorFactory();

    @Override
    public ValueVector createValueVector(List<Row> batch, Column column) {
        int size = batch.size();
        int columnId = column.getColumnId();
        switch (column.getType()) {
        case Value.INT: {
            int[] values = new int[size];
            for (int i = 0; i < size; i++) {
                values[i] = batch.get(i).getValue(columnId).getInt();
            }
            return new IntVector(values);
        }
        case Value.STRING:
        case Value.STRING_FIXED:
        case Value.STRING_IGNORECASE: {
            String[] values = new String[size];
            for (int i = 0; i < size; i++) {
                values[i] = batch.get(i).getValue(columnId).getString();
            }
            return new StringVector(values);
        }
        default:
            return createDefaultValueVector(batch, column);
        }
    }

    public static DefaultValueVector createDefaultValueVector(List<Row> batch, Column column) {
        int size = batch.size();
        int columnId = column.getColumnId();
        Value[] values = new Value[size];
        for (int i = 0; i < size; i++) {
            values[i] = batch.get(i).getValue(columnId);
        }
        return new DefaultValueVector(values);
    }
}
