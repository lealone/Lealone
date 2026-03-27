/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.orm.format;

public interface JsonFormat {

    public static final JsonFormat DEFAULT_FORMAT = new DefaultJsonFormat();

    public static final JsonFormat FRONTEND_FORMAT = new FrontendJsonFormat();

    public static final JsonFormat LOWER_UNDERSCORE_FORMAT = new LowerUnderscoreJsonFormat();

    public boolean includesInternalFields();

    public PropertyFormat getPropertyFormat();

    public NameCaseFormat getNameCaseFormat();

    public ArrayFormat getArrayFormat();

    public BigDecimalFormat getBigDecimalFormat();

    public BlobFormat getBlobFormat();

    public BooleanFormat getBooleanFormat();

    public ByteFormat getByteFormat();

    public BytesFormat getBytesFormat();

    public ClobFormat getClobFormat();

    public DateFormat getDateFormat();

    public DoubleFormat getDoubleFormat();

    public FloatFormat getFloatFormat();

    public IntegerFormat getIntegerFormat();

    public LongFormat getLongFormat();

    public ObjectFormat getObjectFormat();

    public ShortFormat getShortFormat();

    public StringFormat getStringFormat();

    public TimeFormat getTimeFormat();

    public TimestampFormat getTimestampFormat();

    public UuidFormat getUuidFormat();

    public <E> ListFormat<E> getListFormat();

    public <E> SetFormat<E> getSetFormat();

    public <K, V> MapFormat<K, V> getMapFormat();
}
