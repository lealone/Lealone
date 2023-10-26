/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.DateTimeUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Database;
import org.lealone.db.Mode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.db.value.ValueTime;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;

/**
 * Implementation of the date and time functions.
 * 
 * @author H2 Group
 * @author zhh
 */
public class DateTimeFunction extends BuiltInFunction {

    public static final int CURDATE = 100, CURTIME = 101, DATE_ADD = 102, DATE_DIFF = 103,
            DAY_NAME = 104, DAY_OF_MONTH = 105, DAY_OF_WEEK = 106, DAY_OF_YEAR = 107, HOUR = 108,
            MINUTE = 109, MONTH = 110, MONTH_NAME = 111, NOW = 112, QUARTER = 113, SECOND = 114,
            WEEK = 115, YEAR = 116, CURRENT_DATE = 117, CURRENT_TIME = 118, CURRENT_TIMESTAMP = 119,
            EXTRACT = 120, FORMATDATETIME = 121, PARSEDATETIME = 122, ISO_YEAR = 123, ISO_WEEK = 124,
            ISO_DAY_OF_WEEK = 125;

    private static final HashMap<String, Integer> DATE_PART = new HashMap<>();

    public static void init() {
        // DATE_PART
        DATE_PART.put("SQL_TSI_YEAR", Calendar.YEAR);
        DATE_PART.put("YEAR", Calendar.YEAR);
        DATE_PART.put("YYYY", Calendar.YEAR);
        DATE_PART.put("YY", Calendar.YEAR);
        DATE_PART.put("SQL_TSI_MONTH", Calendar.MONTH);
        DATE_PART.put("MONTH", Calendar.MONTH);
        DATE_PART.put("MM", Calendar.MONTH);
        DATE_PART.put("M", Calendar.MONTH);
        DATE_PART.put("SQL_TSI_WEEK", Calendar.WEEK_OF_YEAR);
        DATE_PART.put("WW", Calendar.WEEK_OF_YEAR);
        DATE_PART.put("WK", Calendar.WEEK_OF_YEAR);
        DATE_PART.put("WEEK", Calendar.WEEK_OF_YEAR);
        DATE_PART.put("DAY", Calendar.DAY_OF_MONTH);
        DATE_PART.put("DD", Calendar.DAY_OF_MONTH);
        DATE_PART.put("D", Calendar.DAY_OF_MONTH);
        DATE_PART.put("SQL_TSI_DAY", Calendar.DAY_OF_MONTH);
        DATE_PART.put("DAYOFYEAR", Calendar.DAY_OF_YEAR);
        DATE_PART.put("DAY_OF_YEAR", Calendar.DAY_OF_YEAR);
        DATE_PART.put("DY", Calendar.DAY_OF_YEAR);
        DATE_PART.put("DOY", Calendar.DAY_OF_YEAR);
        DATE_PART.put("SQL_TSI_HOUR", Calendar.HOUR_OF_DAY);
        DATE_PART.put("HOUR", Calendar.HOUR_OF_DAY);
        DATE_PART.put("HH", Calendar.HOUR_OF_DAY);
        DATE_PART.put("SQL_TSI_MINUTE", Calendar.MINUTE);
        DATE_PART.put("MINUTE", Calendar.MINUTE);
        DATE_PART.put("MI", Calendar.MINUTE);
        DATE_PART.put("N", Calendar.MINUTE);
        DATE_PART.put("SQL_TSI_SECOND", Calendar.SECOND);
        DATE_PART.put("SECOND", Calendar.SECOND);
        DATE_PART.put("SS", Calendar.SECOND);
        DATE_PART.put("S", Calendar.SECOND);
        DATE_PART.put("MILLISECOND", Calendar.MILLISECOND);
        DATE_PART.put("MS", Calendar.MILLISECOND);

        addFunctionNotDeterministic("CURRENT_DATE", CURRENT_DATE, 0, Value.DATE);
        addFunctionNotDeterministic("CURDATE", CURDATE, 0, Value.DATE);
        // alias for MSSQLServer
        addFunctionNotDeterministic("GETDATE", CURDATE, 0, Value.DATE);
        addFunctionNotDeterministic("CURRENT_TIME", CURRENT_TIME, 0, Value.TIME);
        addFunctionNotDeterministic("CURTIME", CURTIME, 0, Value.TIME);
        addFunctionNotDeterministic("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP, VAR_ARGS, Value.TIMESTAMP);
        addFunctionNotDeterministic("NOW", NOW, VAR_ARGS, Value.TIMESTAMP);
        addFunction("DATEADD", DATE_ADD, 3, Value.TIMESTAMP);
        addFunction("TIMESTAMPADD", DATE_ADD, 3, Value.LONG);
        addFunction("DATEDIFF", DATE_DIFF, 3, Value.LONG);
        addFunction("TIMESTAMPDIFF", DATE_DIFF, 3, Value.LONG);
        addFunction("DAYNAME", DAY_NAME, 1, Value.STRING);
        addFunction("DAY", DAY_OF_MONTH, 1, Value.INT);
        addFunction("DAY_OF_MONTH", DAY_OF_MONTH, 1, Value.INT);
        addFunction("DAY_OF_WEEK", DAY_OF_WEEK, 1, Value.INT);
        addFunction("DAY_OF_YEAR", DAY_OF_YEAR, 1, Value.INT);
        addFunction("DAYOFMONTH", DAY_OF_MONTH, 1, Value.INT);
        addFunction("DAYOFWEEK", DAY_OF_WEEK, 1, Value.INT);
        addFunction("DAYOFYEAR", DAY_OF_YEAR, 1, Value.INT);
        addFunction("HOUR", HOUR, 1, Value.INT);
        addFunction("MINUTE", MINUTE, 1, Value.INT);
        addFunction("MONTH", MONTH, 1, Value.INT);
        addFunction("MONTHNAME", MONTH_NAME, 1, Value.STRING);
        addFunction("QUARTER", QUARTER, 1, Value.INT);
        addFunction("SECOND", SECOND, 1, Value.INT);
        addFunction("WEEK", WEEK, 1, Value.INT);
        addFunction("YEAR", YEAR, 1, Value.INT);
        addFunction("EXTRACT", EXTRACT, 2, Value.INT);
        addFunctionWithNull("FORMATDATETIME", FORMATDATETIME, VAR_ARGS, Value.STRING);
        addFunctionWithNull("PARSEDATETIME", PARSEDATETIME, VAR_ARGS, Value.TIMESTAMP);
        addFunction("ISO_YEAR", ISO_YEAR, 1, Value.INT);
        addFunction("ISO_WEEK", ISO_WEEK, 1, Value.INT);
        addFunction("ISO_DAY_OF_WEEK", ISO_DAY_OF_WEEK, 1, Value.INT);
    }

    /**
     * Check if a given string is a valid date part string.
     *
     * @param part the string
     * @return true if it is
     */
    public static boolean isDatePart(String part) {
        Integer p = DATE_PART.get(StringUtils.toUpperEnglish(part));
        return p != null;
    }

    protected DateTimeFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected Value getValue0(ServerSession session) {
        Value result;
        switch (info.type) {
        case CURDATE:
        case CURRENT_DATE: {
            long now = session.getTransactionStart();
            // need to normalize
            result = ValueDate.get(new Date(now));
            break;
        }
        case CURTIME:
        case CURRENT_TIME: {
            long now = session.getTransactionStart();
            // need to normalize
            result = ValueTime.get(new Time(now));
            break;
        }
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    @Override
    protected Value getValue1(ServerSession session, Value v) {
        Value result;
        switch (info.type) {
        case DAY_NAME: {
            SimpleDateFormat dayName = new SimpleDateFormat("EEEE", Locale.ENGLISH);
            result = ValueString.get(dayName.format(v.getDate()));
            break;
        }
        case DAY_OF_MONTH:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getDate(), Calendar.DAY_OF_MONTH));
            break;
        case DAY_OF_WEEK:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getDate(), Calendar.DAY_OF_WEEK));
            break;
        case DAY_OF_YEAR:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getDate(), Calendar.DAY_OF_YEAR));
            break;
        case HOUR:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getTimestamp(), Calendar.HOUR_OF_DAY));
            break;
        case MINUTE:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getTimestamp(), Calendar.MINUTE));
            break;
        case MONTH:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getDate(), Calendar.MONTH));
            break;
        case MONTH_NAME: {
            SimpleDateFormat monthName = new SimpleDateFormat("MMMM", Locale.ENGLISH);
            result = ValueString.get(monthName.format(v.getDate()));
            break;
        }
        case QUARTER:
            result = ValueInt.get((DateTimeUtils.getDatePart(v.getDate(), Calendar.MONTH) - 1) / 3 + 1);
            break;
        case SECOND:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getTimestamp(), Calendar.SECOND));
            break;
        case WEEK:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getDate(), Calendar.WEEK_OF_YEAR));
            break;
        case YEAR:
            result = ValueInt.get(DateTimeUtils.getDatePart(v.getDate(), Calendar.YEAR));
            break;
        case ISO_YEAR:
            result = ValueInt.get(DateTimeUtils.getIsoYear(v.getDate()));
            break;
        case ISO_WEEK:
            result = ValueInt.get(DateTimeUtils.getIsoWeek(v.getDate()));
            break;
        case ISO_DAY_OF_WEEK:
            result = ValueInt.get(DateTimeUtils.getIsoDayOfWeek(v.getDate()));
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    @Override
    protected Value getValueN(ServerSession session, Expression[] args, Value[] values) {
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value v2 = getNullOrValue(session, args, values, 2);
        Value v3 = getNullOrValue(session, args, values, 3);
        Value result;
        switch (info.type) {
        case DATE_ADD:
            // 月份加1，结果是2001-02-28 00:00:00.0
            // sql = "SELECT DATEADD('MONTH', 1, DATE '2001-01-31')";
            result = ValueTimestamp.get(dateadd(v0.getString(), v1.getInt(), v2.getTimestamp()));
            break;
        case DATE_DIFF:
            // 用后面的YEAR减去前面的YEAR，1999-2001=-2
            // sql = "SELECT DATEDIFF('YEAR', DATE '2001-01-31', DATE '1999-01-31')";
            result = ValueLong.get(datediff(v0.getString(), v1.getTimestamp(), v2.getTimestamp()));
            break;
        case EXTRACT: {
            // 抽取日期和年份 CURRENT_TIMESTAMP=2012-12-03 22:20:08.597 DAY=3 YEAR=2012
            // sql = "SELECT CURRENT_TIMESTAMP, EXTRACT(DAY FROM CURRENT_TIMESTAMP), EXTRACT(YEAR FROM
            // CURRENT_TIMESTAMP)";
            int field = getDatePart(v0.getString());
            result = ValueInt.get(DateTimeUtils.getDatePart(v1.getTimestamp(), field));
            break;
        }
        case FORMATDATETIME: {
            // format datetime 格式化日期时间
            // sql = "SELECT FORMATDATETIME(TIMESTAMP '2001-02-03 04:05:06', 'EEE, d MMM yyyy HH:mm:ss z', 'en',
            // 'GMT')";

            // v0 timestamp = TIMESTAMP '2001-02-03 04:05:06' ,
            // v1 formatString = 'EEE, d MMM yyyy HH:mm:ss z'
            // v2 localeString = 'en'
            // v3 timeZoneString = 'GMT'
            // 结果 Fri, 2 Feb 2001 20:05:06 GMT

            // vo和v1只要一个为null，结果为null

            if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
            } else {
                String locale = v2 == null ? null : v2 == ValueNull.INSTANCE ? null : v2.getString();
                String tz = v3 == null ? null : v3 == ValueNull.INSTANCE ? null : v3.getString();
                result = ValueString.get(
                        DateTimeUtils.formatDateTime(v0.getTimestamp(), v1.getString(), locale, tz));
            }
            break;
        }
        case PARSEDATETIME: {
            // parse datetime解析日期时间
            // 按后面三个参数指定的格式解析第一个参数，得到一个java.util.Date
            // 结果: 2001-02-03 11:05:06.0
            // sql = "SELECT PARSEDATETIME('Sat, 3 Feb 2001 03:05:06 GMT', 'EEE, d MMM yyyy HH:mm:ss z', 'en', 'GMT')";

            // 同上
            if (v0 == ValueNull.INSTANCE || v1 == ValueNull.INSTANCE) {
                result = ValueNull.INSTANCE;
            } else {
                String locale = v2 == null ? null : v2 == ValueNull.INSTANCE ? null : v2.getString();
                String tz = v3 == null ? null : v3 == ValueNull.INSTANCE ? null : v3.getString();
                java.util.Date d = DateTimeUtils.parseDateTime(v0.getString(), v1.getString(), locale,
                        tz);
                result = ValueTimestamp.get(new Timestamp(d.getTime()));
            }
            break;
        }
        case NOW:
        case CURRENT_TIMESTAMP: { // 参数可变
            long now = session.getTransactionStart();
            ValueTimestamp vt = ValueTimestamp.get(new Timestamp(now));
            // NOW(1)表示毫秒数只保留一位，如NOW()="2012-12-03 22:03:44.647" 则NOW(1)="2012-12-03 22:03:44.6"
            // 毫秒数一般是3位，如果NOW(100)，100>3了，所以NOW(100)跟NOW()一样
            if (v0 != null) {
                Mode mode = database.getMode();
                // ValueTimestamp.convertScale(boolean, int)忽视convertOnlyToSmallerScale参数
                // 所以convertOnlyToSmallerScale没用到
                vt = (ValueTimestamp) vt.convertScale(mode.convertOnlyToSmallerScale, v0.getInt());
            }
            result = vt;
            break;
        }
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private static int getDatePart(String part) {
        Integer p = DATE_PART.get(StringUtils.toUpperEnglish(part));
        if (p == null) {
            throw DbException.getInvalidValueException("date part", part);
        }
        return p.intValue();
    }

    private static Timestamp dateadd(String part, int count, Timestamp d) {
        int field = getDatePart(part);
        Calendar calendar = Calendar.getInstance();
        int nanos = d.getNanos() % 1000000;
        calendar.setTime(d);
        calendar.add(field, count);
        long t = calendar.getTime().getTime();
        Timestamp ts = new Timestamp(t);
        ts.setNanos(ts.getNanos() + nanos);
        return ts;
    }

    /**
     * Calculate the number of crossed unit boundaries between two timestamps.
     * This method is supported for MS SQL Server compatibility.
     * <pre>
     * DATEDIFF(YEAR, '2004-12-31', '2005-01-01') = 1
     * </pre>
     *
     * @param part the part
     * @param d1 the first date
     * @param d2 the second date
     * @return the number of crossed boundaries
     */
    private static long datediff(String part, Timestamp d1, Timestamp d2) {
        int field = getDatePart(part);
        Calendar calendar = Calendar.getInstance();
        long t1 = d1.getTime(), t2 = d2.getTime();
        // need to convert to UTC, otherwise we get inconsistent results with
        // certain time zones (those that are 30 minutes off)
        TimeZone zone = calendar.getTimeZone();
        calendar.setTime(d1);
        t1 += zone.getOffset(calendar.get(Calendar.ERA), calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.DAY_OF_WEEK), calendar.get(Calendar.MILLISECOND));
        calendar.setTime(d2);
        t2 += zone.getOffset(calendar.get(Calendar.ERA), calendar.get(Calendar.YEAR),
                calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH),
                calendar.get(Calendar.DAY_OF_WEEK), calendar.get(Calendar.MILLISECOND));
        switch (field) {
        case Calendar.MILLISECOND:
            return t2 - t1;
        case Calendar.SECOND:
        case Calendar.MINUTE:
        case Calendar.HOUR_OF_DAY: {
            // first 'normalize' the numbers so both are not negative
            long hour = 60 * 60 * 1000;
            long add = Math.min(t1 / hour * hour, t2 / hour * hour);
            t1 -= add;
            t2 -= add;
            switch (field) {
            case Calendar.SECOND:
                return t2 / 1000 - t1 / 1000;
            case Calendar.MINUTE:
                return t2 / (60 * 1000) - t1 / (60 * 1000);
            case Calendar.HOUR_OF_DAY:
                return t2 / hour - t1 / hour;
            default:
                throw DbException.getInternalError("field:" + field);
            }
        }
        case Calendar.DATE:
            return t2 / (24 * 60 * 60 * 1000) - t1 / (24 * 60 * 60 * 1000);
        default:
            break;
        }
        calendar.setTime(new Timestamp(t1));
        int year1 = calendar.get(Calendar.YEAR);
        int month1 = calendar.get(Calendar.MONTH);
        calendar.setTime(new Timestamp(t2));
        int year2 = calendar.get(Calendar.YEAR);
        int month2 = calendar.get(Calendar.MONTH);
        int result = year2 - year1;
        if (field == Calendar.MONTH) {
            result = 12 * result + (month2 - month1);
        }
        return result;
    }

    @Override
    protected void checkParameterCount(int len) {
        int min = 0, max = Integer.MAX_VALUE;
        switch (info.type) {
        case NOW:
        case CURRENT_TIMESTAMP:
            max = 1;
            break;
        case FORMATDATETIME:
        case PARSEDATETIME:
            min = 2;
            max = 4;
            break;
        default:
            DbException.throwInternalError("type=" + info.type);
        }
        checkParameterCount(len, min, max);
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder(info.name);
        buff.append('(');
        switch (info.type) {
        case EXTRACT: {
            ValueString v = (ValueString) ((ValueExpression) args[0]).getValue(null);
            buff.append(v.getString()).append(" FROM ").append(args[1].getSQL());
            break;
        }
        default:
            appendArgs(buff);
        }
        return buff.append(')').toString();
    }

    @Override
    protected void calculatePrecisionAndDisplaySize() {
        switch (info.type) {
        case DAY_NAME:
        case MONTH_NAME:
            // day and month names may be long in some languages
            precision = 20;
            displaySize = (int) precision;
            break;
        default:
            super.calculatePrecisionAndDisplaySize();
        }
    }
}
