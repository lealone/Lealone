/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.vector;

import java.util.List;

import org.lealone.db.result.Row;
import org.lealone.db.table.Column;

public interface ValueVectorFactory {

    public ValueVector createValueVector(List<Row> batch, Column column);

}
