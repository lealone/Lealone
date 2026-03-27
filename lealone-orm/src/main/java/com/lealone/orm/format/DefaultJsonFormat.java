/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public class DefaultJsonFormat implements JsonFormat {

    public static final ArrayFormat ARRAY_FORMAT = new ArrayFormat();

    public static final BigDecimalFormat BIG_DECIMAL_FORMAT = new BigDecimalFormat();

    public static final BlobFormat BLOB_FORMAT = new BlobFormat();

    public static final BooleanFormat BOOLEAN_FORMAT = new BooleanFormat();

    public static final ByteFormat BYTE_FORMAT = new ByteFormat();

    public static final BytesFormat BYTES_FORMAT = new BytesFormat();

    public static final ClobFormat CLOB_FORMAT = new ClobFormat();

    public static final DateFormat DATE_FORMAT = new DateFormat();

    public static final DoubleFormat DOUBLE_FORMAT = new DoubleFormat();

    public static final FloatFormat FLOAT_FORMAT = new FloatFormat();

    public static final IntegerFormat INTEGER_FORMAT = new IntegerFormat();

    public static final LongFormat LONG_FORMAT = new LongFormat();

    public static final ObjectFormat OBJECT_FORMAT = new ObjectFormat();

    public static final ShortFormat SHORT_FORMAT = new ShortFormat();

    public static final StringFormat STRING_FORMAT = new StringFormat();

    public static final TimeFormat TIME_FORMAT = new TimeFormat();

    public static final TimestampFormat TIMESTAMP_FORMAT = new TimestampFormat();

    public static final UuidFormat UUID_FORMAT = new UuidFormat();

    public static final NameCaseFormat NAME_CASE_FORMAT = new NameCaseFormat();

    public static final ListFormat<?> LIST_FORMAT = new ListFormat<>();

    public static final SetFormat<?> SET_FORMAT = new SetFormat<>();

    public static final MapFormat<?, ?> MAP_FORMAT = new MapFormat<>();

    @Override
    public boolean includesInternalFields() {
        return true;
    }

    @Override
    public PropertyFormat getPropertyFormat() {
        return null;
    }

    @Override
    public NameCaseFormat getNameCaseFormat() {
        return NAME_CASE_FORMAT;
    }

    @Override
    public ArrayFormat getArrayFormat() {
        return ARRAY_FORMAT;
    }

    @Override
    public BigDecimalFormat getBigDecimalFormat() {
        return BIG_DECIMAL_FORMAT;
    }

    @Override
    public BlobFormat getBlobFormat() {
        return BLOB_FORMAT;
    }

    @Override
    public BooleanFormat getBooleanFormat() {
        return BOOLEAN_FORMAT;
    }

    @Override
    public ByteFormat getByteFormat() {
        return BYTE_FORMAT;
    }

    @Override
    public BytesFormat getBytesFormat() {
        return BYTES_FORMAT;
    }

    @Override
    public ClobFormat getClobFormat() {
        return CLOB_FORMAT;
    }

    @Override
    public DateFormat getDateFormat() {
        return DATE_FORMAT;
    }

    @Override
    public DoubleFormat getDoubleFormat() {
        return DOUBLE_FORMAT;
    }

    @Override
    public FloatFormat getFloatFormat() {
        return FLOAT_FORMAT;
    }

    @Override
    public IntegerFormat getIntegerFormat() {
        return INTEGER_FORMAT;
    }

    @Override
    public LongFormat getLongFormat() {
        return LONG_FORMAT;
    }

    @Override
    public ObjectFormat getObjectFormat() {
        return OBJECT_FORMAT;
    }

    @Override
    public ShortFormat getShortFormat() {
        return SHORT_FORMAT;
    }

    @Override
    public StringFormat getStringFormat() {
        return STRING_FORMAT;
    }

    @Override
    public TimeFormat getTimeFormat() {
        return TIME_FORMAT;
    }

    @Override
    public TimestampFormat getTimestampFormat() {
        return TIMESTAMP_FORMAT;
    }

    @Override
    public UuidFormat getUuidFormat() {
        return UUID_FORMAT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> ListFormat<E> getListFormat() {
        return (ListFormat<E>) LIST_FORMAT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <E> SetFormat<E> getSetFormat() {
        return (SetFormat<E>) SET_FORMAT;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> MapFormat<K, V> getMapFormat() {
        return (MapFormat<K, V>) MAP_FORMAT;
    }
}
