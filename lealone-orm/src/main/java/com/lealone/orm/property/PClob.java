/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.property;

import java.sql.Clob;
import java.sql.SQLException;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.value.Value;
import com.lealone.db.value.ValueString;
import com.lealone.orm.Model;
import com.lealone.orm.format.ClobFormat;
import com.lealone.orm.format.JsonFormat;

public class PClob<M extends Model<M>> extends PBase<M, Clob> {

    public PClob(String name, M model) {
        super(name, model);
    }

    public static String toString(Clob v) {
        try {
            return v.getSubString(1, (int) v.length());
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    protected ClobFormat getValueFormat(JsonFormat format) {
        return format.getClobFormat();
    }

    @Override
    protected Value createValue(Clob value) {
        return ValueString.get(toString(value).toString());
    }

    @Override
    protected void deserialize(Value v) {
        value = v.getClob();
    }
}
