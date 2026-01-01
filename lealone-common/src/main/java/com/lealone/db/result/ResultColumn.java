/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.result;

import java.io.IOException;

import com.lealone.net.NetInputStream;
import com.lealone.net.NetOutputStream;

public class ResultColumn {

    /**
     * The column alias.
     */
    public final String alias;

    /**
     * The schema name or null.
     */
    public final String schemaName;

    /**
     * The table name or null.
     */
    public final String tableName;

    /**
     * The column name or null.
     */
    public final String columnName;

    /**
     * The value type of this column.
     */
    public final int columnType;

    /**
     * The precision.
     */
    public final long precision;

    /**
     * The scale.
     */
    public final int scale;

    /**
     * The expected display size.
     */
    public final int displaySize;

    /**
     * True if this is an auto-increment column.
     */
    public final boolean autoIncrement;

    /**
     * True if this column is nullable.
     */
    public final int nullable;

    /**
     * Read an object from the given transfer object.
     *
     * @param in the object from where to read the data
     */
    private ResultColumn(NetInputStream in) throws IOException {
        alias = in.readString();
        schemaName = in.readString();
        tableName = in.readString();
        columnName = in.readString();
        columnType = in.readInt();
        precision = in.readLong();
        scale = in.readInt();
        displaySize = in.readInt();
        autoIncrement = in.readBoolean();
        nullable = in.readInt();
    }

    public static ResultColumn read(NetInputStream in) throws IOException {
        return new ResultColumn(in);
    }

    /**
    * Write a result column to the given output.
    *
    * @param result the result
    * @param i the column index
    */
    public static void write(NetOutputStream out, Result result, int i) throws IOException {
        out.writeString(result.getAlias(i));
        out.writeString(result.getSchemaName(i));
        out.writeString(result.getTableName(i));
        out.writeString(result.getColumnName(i));
        out.writeInt(result.getColumnType(i));
        out.writeLong(result.getColumnPrecision(i));
        out.writeInt(result.getColumnScale(i));
        out.writeInt(result.getDisplaySize(i));
        out.writeBoolean(result.isAutoIncrement(i));
        out.writeInt(result.getNullable(i));
    }
}
