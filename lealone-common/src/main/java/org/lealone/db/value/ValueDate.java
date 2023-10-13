/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.db.value;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.DataBuffer;
import org.lealone.db.api.ErrorCode;
import org.lealone.storage.type.StorageDataTypeBase;

/**
 * Implementation of the DATE data type.
 */
public class ValueDate extends Value {

    /**
     * The precision in digits.
     */
    public static final int PRECISION = 8;

    /**
     * The display size of the textual representation of a date.
     * Example: 2000-01-02
     */
    public static final int DISPLAY_SIZE = 10;

    private final long dateValue;

    private ValueDate(long dateValue) {
        this.dateValue = dateValue;
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param dateValue the date value
     * @return the value
     */
    public static ValueDate fromDateValue(long dateValue) {
        return (ValueDate) Value.cache(new ValueDate(dateValue));
    }

    /**
     * Get or create a date value for the given date.
     *
     * @param date the date
     * @return the value
     */
    public static ValueDate get(Date date) {
        return fromDateValue(DateTimeUtils.dateValueFromDate(date.getTime()));
    }

    /**
     * Parse a string to a ValueDate.
     *
     * @param s the string to parse
     * @return the date
     */
    public static ValueDate parse(String s) {
        try {
            return fromDateValue(DateTimeUtils.parseDateValue(s, 0, s.length()));
        } catch (Exception e) {
            throw DbException.get(ErrorCode.INVALID_DATETIME_CONSTANT_2, e, "DATE", s);
        }
    }

    public long getDateValue() {
        return dateValue;
    }

    @Override
    public Date getDate() {
        return DateTimeUtils.convertDateValueToDate(dateValue);
    }

    @Override
    public int getType() {
        return Value.DATE;
    }

    @Override
    public String getString() {
        StringBuilder buff = new StringBuilder(DISPLAY_SIZE);
        appendDate(buff, dateValue);
        return buff.toString();
    }

    @Override
    public String getSQL() {
        return "DATE '" + getString() + "'";
    }

    @Override
    public long getPrecision() {
        return PRECISION;
    }

    @Override
    public int getDisplaySize() {
        return DISPLAY_SIZE;
    }

    @Override
    protected int compareSecure(Value o, CompareMode mode) {
        return MathUtils.compareLong(dateValue, ((ValueDate) o).dateValue);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        return other instanceof ValueDate && dateValue == (((ValueDate) other).dateValue);
    }

    @Override
    public int hashCode() {
        return (int) (dateValue ^ (dateValue >>> 32));
    }

    @Override
    public Object getObject() {
        return getDate();
    }

    @Override
    public void set(PreparedStatement prep, int parameterIndex) throws SQLException {
        prep.setDate(parameterIndex, getDate());
    }

    /**
     * Append a date to the string builder.
     *
     * @param buff the target string builder
     * @param dateValue the date value
     */
    static void appendDate(StringBuilder buff, long dateValue) {
        int y = DateTimeUtils.yearFromDateValue(dateValue);
        int m = DateTimeUtils.monthFromDateValue(dateValue);
        int d = DateTimeUtils.dayFromDateValue(dateValue);
        if (y > 0 && y < 10000) {
            StringUtils.appendZeroPadded(buff, 4, y);
        } else {
            buff.append(y);
        }
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, m);
        buff.append('-');
        StringUtils.appendZeroPadded(buff, 2, d);
    }

    public static final StorageDataTypeBase type = new StorageDataTypeBase() {

        @Override
        public int getType() {
            return DATE;
        }

        @Override
        public int compare(Object aObj, Object bObj) {
            Date a = (Date) aObj;
            Date b = (Date) bObj;
            return a.compareTo(b);
        }

        @Override
        public int getMemory(Object obj) {
            return 24;
        }

        @Override
        public void write(DataBuffer buff, Object obj) {
            Date d = (Date) obj;
            write0(buff, d.getTime());
        }

        @Override
        public void writeValue(DataBuffer buff, Value v) {
            long d = ((ValueDate) v).getDateValue();
            write0(buff, d);
        }

        private void write0(DataBuffer buff, long v) {
            buff.put((byte) Value.DATE).putVarLong(v);
        }

        @Override
        public Value readValue(ByteBuffer buff) {
            return ValueDate.fromDateValue(DataUtils.readVarLong(buff));
        }
    };
}
