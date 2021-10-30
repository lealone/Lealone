/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector.jdk16;

import java.util.List;

import org.lealone.db.result.Row;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;
import org.lealone.sql.vector.DefaultValueVectorFactory;
import org.lealone.sql.vector.ValueVector;
import org.lealone.sql.vector.ValueVectorFactory;

public class Jdk16ValueVectorFactory implements ValueVectorFactory {

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
            return new Jdk16IntVector(values);
        }
        default:
            return DefaultValueVectorFactory.createDefaultValueVector(batch, column);
        }
    }

}
