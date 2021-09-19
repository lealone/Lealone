/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import java.util.regex.PatternSyntaxException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.MathUtils;
import org.lealone.common.util.StringUtils;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;

/**
 * Implementation of the string functions.
 * 
 * @author H2 Group
 * @author zhh
 */
public class StringFunction extends BuiltInFunction {

    public static void init() {
    }

    public static final int ASCII = 50, BIT_LENGTH = 51, CHAR = 52, CHAR_LENGTH = 53, CONCAT = 54, DIFFERENCE = 55,
            HEXTORAW = 56, INSERT = 57, INSTR = 58, LCASE = 59, LEFT = 60, LENGTH = 61, LOCATE = 62, LTRIM = 63,
            OCTET_LENGTH = 64, RAWTOHEX = 65, REPEAT = 66, REPLACE = 67, RIGHT = 68, RTRIM = 69, SOUNDEX = 70,
            SPACE = 71, SUBSTR = 72, SUBSTRING = 73, UCASE = 74, LOWER = 75, UPPER = 76, POSITION = 77, TRIM = 78,
            STRINGENCODE = 79, STRINGDECODE = 80, STRINGTOUTF8 = 81, UTF8TOSTRING = 82, XMLATTR = 83, XMLNODE = 84,
            XMLCOMMENT = 85, XMLCDATA = 86, XMLSTARTDOC = 87, XMLTEXT = 88, REGEXP_REPLACE = 89, RPAD = 90, LPAD = 91,
            CONCAT_WS = 92;

    private static final char[] SOUNDEX_INDEX = new char[128];

    static {
        // SOUNDEX_INDEX
        String index = "7AEIOUY8HW1BFPV2CGJKQSXZ3DT4L5MN6R";
        char number = 0;
        for (int i = 0, length = index.length(); i < length; i++) {
            char c = index.charAt(i);
            if (c < '9') {
                number = c;
            } else {
                SOUNDEX_INDEX[c] = number;
                SOUNDEX_INDEX[Character.toLowerCase(c)] = number;
            }
        }
        // string
        addFunction("ASCII", ASCII, 1, Value.INT);
        addFunction("BIT_LENGTH", BIT_LENGTH, 1, Value.LONG);
        addFunction("CHAR", CHAR, 1, Value.STRING);
        addFunction("CHR", CHAR, 1, Value.STRING);
        addFunction("CHAR_LENGTH", CHAR_LENGTH, 1, Value.INT);
        // same as CHAR_LENGTH
        addFunction("CHARACTER_LENGTH", CHAR_LENGTH, 1, Value.INT);
        addFunctionWithNull("CONCAT", CONCAT, VAR_ARGS, Value.STRING);
        addFunctionWithNull("CONCAT_WS", CONCAT_WS, VAR_ARGS, Value.STRING);
        addFunction("DIFFERENCE", DIFFERENCE, 2, Value.INT);
        addFunction("HEXTORAW", HEXTORAW, 1, Value.STRING);
        addFunctionWithNull("INSERT", INSERT, 4, Value.STRING);
        addFunction("LCASE", LCASE, 1, Value.STRING);
        addFunction("LEFT", LEFT, 2, Value.STRING);
        addFunction("LENGTH", LENGTH, 1, Value.LONG);
        // 2 or 3 arguments
        addFunction("LOCATE", LOCATE, VAR_ARGS, Value.INT);
        // alias for MSSQLServer
        addFunction("CHARINDEX", LOCATE, VAR_ARGS, Value.INT);
        // same as LOCATE with 2 arguments
        addFunction("POSITION", LOCATE, 2, Value.INT);
        addFunction("INSTR", INSTR, VAR_ARGS, Value.INT);
        addFunction("LTRIM", LTRIM, VAR_ARGS, Value.STRING);
        addFunction("OCTET_LENGTH", OCTET_LENGTH, 1, Value.LONG);
        addFunction("RAWTOHEX", RAWTOHEX, 1, Value.STRING);
        addFunction("REPEAT", REPEAT, 2, Value.STRING);
        addFunction("REPLACE", REPLACE, VAR_ARGS, Value.STRING);
        addFunction("RIGHT", RIGHT, 2, Value.STRING);
        addFunction("RTRIM", RTRIM, VAR_ARGS, Value.STRING);
        addFunction("SOUNDEX", SOUNDEX, 1, Value.STRING);
        addFunction("SPACE", SPACE, 1, Value.STRING);
        addFunction("SUBSTR", SUBSTR, VAR_ARGS, Value.STRING);
        addFunction("SUBSTRING", SUBSTRING, VAR_ARGS, Value.STRING);
        addFunction("UCASE", UCASE, 1, Value.STRING);
        addFunction("LOWER", LOWER, 1, Value.STRING);
        addFunction("UPPER", UPPER, 1, Value.STRING);
        addFunction("POSITION", POSITION, 2, Value.INT);
        addFunction("TRIM", TRIM, VAR_ARGS, Value.STRING);
        addFunction("STRINGENCODE", STRINGENCODE, 1, Value.STRING);
        addFunction("STRINGDECODE", STRINGDECODE, 1, Value.STRING);
        addFunction("STRINGTOUTF8", STRINGTOUTF8, 1, Value.BYTES);
        addFunction("UTF8TOSTRING", UTF8TOSTRING, 1, Value.STRING);
        addFunction("XMLATTR", XMLATTR, 2, Value.STRING);
        addFunctionWithNull("XMLNODE", XMLNODE, VAR_ARGS, Value.STRING);
        addFunction("XMLCOMMENT", XMLCOMMENT, 1, Value.STRING);
        addFunction("XMLCDATA", XMLCDATA, 1, Value.STRING);
        addFunction("XMLSTARTDOC", XMLSTARTDOC, 0, Value.STRING);
        addFunction("XMLTEXT", XMLTEXT, VAR_ARGS, Value.STRING);
        addFunction("REGEXP_REPLACE", REGEXP_REPLACE, 3, Value.STRING);
        addFunction("RPAD", RPAD, VAR_ARGS, Value.STRING);
        addFunction("LPAD", LPAD, VAR_ARGS, Value.STRING);
    }

    protected StringFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected Value getValue0(ServerSession session) {
        Value result;
        switch (info.type) {
        case XMLSTARTDOC:
            result = ValueString.get(StringUtils.xmlStartDoc());
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    @Override
    protected Value getValue1(ServerSession session, Value v) {
        Value result;
        switch (info.type) {
        case ASCII: {
            String s = v.getString();
            if (s.length() == 0) {
                result = ValueNull.INSTANCE;
            } else {
                result = ValueInt.get(s.charAt(0));
            }
            break;
        }
        case BIT_LENGTH:
            result = ValueLong.get(16 * length(v));
            break;
        case CHAR:
            result = ValueString.get(String.valueOf((char) v.getInt()));
            break;
        case CHAR_LENGTH:
        case LENGTH:
            result = ValueLong.get(length(v));
            break;
        case OCTET_LENGTH:
            result = ValueLong.get(2 * length(v));
            break;
        case HEXTORAW:
            result = ValueString.get(hexToRaw(v.getString()));
            break;
        case LOWER:
        case LCASE:
            // TODO this is locale specific, need to document or provide a way
            // to set the locale
            result = ValueString.get(v.getString().toLowerCase());
            break;
        case RAWTOHEX:
            result = ValueString.get(rawToHex(v.getString()));
            break;
        case SOUNDEX:
            result = ValueString.get(getSoundex(v.getString()));
            break;
        case SPACE: {
            int len = Math.max(0, v.getInt());
            char[] chars = new char[len];
            for (int i = len - 1; i >= 0; i--) {
                chars[i] = ' ';
            }
            result = ValueString.get(new String(chars));
            break;
        }
        case UPPER:
        case UCASE:
            // TODO this is locale specific, need to document or provide a way
            // to set the locale
            result = ValueString.get(v.getString().toUpperCase());
            break;
        case STRINGENCODE:
            result = ValueString.get(StringUtils.javaEncode(v.getString()));
            break;
        case STRINGDECODE:
            result = ValueString.get(StringUtils.javaDecode(v.getString()));
            break;
        case STRINGTOUTF8:
            result = ValueBytes.getNoCopy(v.getString().getBytes(Constants.UTF8));
            break;
        case UTF8TOSTRING:
            result = ValueString.get(new String(v.getBytesNoCopy(), Constants.UTF8));
            break;
        case XMLCOMMENT:
            result = ValueString.get(StringUtils.xmlComment(v.getString()));
            break;
        case XMLCDATA:
            result = ValueString.get(StringUtils.xmlCData(v.getString()));
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    // 见http://www.archives.gov/publications/general-info-leaflets/55-census.html
    // 34个字符(26个大写字母加1到8这8个数字)
    // 7: AEIOUY 及它们的小写(下同)
    // 8: HW
    // 1: BFPV
    // 2: CGJKQSXZ
    // 3: DT
    // 4: L
    // 5: MN
    // 6: R
    //
    // 算法是: 忽略所有的非字符，然后保留第一个字符，忽略对应7和8的字符，其他的转成对应的数字，重复的不算，不够4位的补0
    // sql = "SELECT SOUNDEX('1aaa')"; //1被去掉，保留第一个a，第二和第三个a对应7被忽略，所以最后是a000
    //
    // B保留，H去掉，C转成2，W去掉，D转成3，H去掉，A去掉，最后是B23因为是3位，所以不够4位，最后是B230
    // sql = "SELECT SOUNDEX('BHCWDHA')";
    private static String getSoundex(String s) {
        int len = s.length();
        char[] chars = { '0', '0', '0', '0' };
        char lastDigit = '0';
        for (int i = 0, j = 0; i < len && j < 4; i++) {
            char c = s.charAt(i);
            char newDigit = c > SOUNDEX_INDEX.length ? 0 : SOUNDEX_INDEX[c];
            if (newDigit != 0) {
                if (j == 0) {
                    chars[j++] = c;
                    lastDigit = newDigit;
                } else if (newDigit <= '6') {
                    if (newDigit != lastDigit) {
                        chars[j++] = newDigit;
                        lastDigit = newDigit;
                    }
                } else if (newDigit == '7') {
                    lastDigit = newDigit;
                }
            }
        }
        return new String(chars);
    }

    private static long length(Value v) {
        switch (v.getType()) {
        case Value.BLOB:
        case Value.CLOB:
        case Value.BYTES:
        case Value.JAVA_OBJECT:
            return v.getPrecision();
        default:
            return v.getString().length();
        }
    }

    private static String hexToRaw(String s) {
        // TODO function hextoraw compatibility with oracle
        int len = s.length();
        if (len % 4 != 0) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
        }
        StringBuilder buff = new StringBuilder(len / 4);
        for (int i = 0; i < len; i += 4) {
            try {
                char raw = (char) Integer.parseInt(s.substring(i, i + 4), 16);
                buff.append(raw);
            } catch (NumberFormatException e) {
                throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1, s);
            }
        }
        return buff.toString();
    }

    private static String rawToHex(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(4 * length);
        for (int i = 0; i < length; i++) {
            String hex = Integer.toHexString(s.charAt(i) & 0xffff);
            for (int j = hex.length(); j < 4; j++) {
                buff.append('0');
            }
            buff.append(hex);
        }
        return buff.toString();
    }

    @Override
    protected Value getValueN(ServerSession session, Expression[] args, Value[] values) {
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value v2 = getNullOrValue(session, args, values, 2);
        Value v3 = getNullOrValue(session, args, values, 3);
        Value result;
        switch (info.type) {
        case DIFFERENCE:
            result = ValueInt.get(getDifference(v0.getString(), v1.getString()));
            break;
        case INSERT: {
            if (v1 == ValueNull.INSTANCE || v2 == ValueNull.INSTANCE) {
                result = v1;
            } else {
                result = ValueString.get(insert(v0.getString(), v1.getInt(), v2.getInt(), v3.getString()));
            }
            break;
        }
        case LEFT:
            result = ValueString.get(left(v0.getString(), v1.getInt()));
            break;
        case LOCATE: {
            int start = v2 == null ? 0 : v2.getInt();
            result = ValueInt.get(locate(v0.getString(), v1.getString(), start));
            break;
        }
        case INSTR: {
            int start = v2 == null ? 0 : v2.getInt();
            result = ValueInt.get(locate(v1.getString(), v0.getString(), start));
            break;
        }
        case REPEAT: {
            int count = Math.max(0, v1.getInt());
            result = ValueString.get(repeat(v0.getString(), count));
            break;
        }
        case REPLACE: {
            String s0 = v0.getString();
            String s1 = v1.getString();
            String s2 = (v2 == null) ? "" : v2.getString();
            result = ValueString.get(replace(s0, s1, s2));
            break;
        }
        case RIGHT:
            result = ValueString.get(right(v0.getString(), v1.getInt()));
            break;
        case LTRIM:
            result = ValueString.get(StringUtils.trim(v0.getString(), true, false, v1 == null ? " " : v1.getString()));
            break;
        case TRIM:
            result = ValueString.get(StringUtils.trim(v0.getString(), true, true, v1 == null ? " " : v1.getString()));
            break;
        case RTRIM:
            result = ValueString.get(StringUtils.trim(v0.getString(), false, true, v1 == null ? " " : v1.getString()));
            break;
        case SUBSTR:
        case SUBSTRING: {
            String s = v0.getString();
            int offset = v1.getInt();
            if (offset < 0) {
                offset = s.length() + offset + 1;
            }
            int length = v2 == null ? s.length() : v2.getInt();
            result = ValueString.get(substring(s, offset, length));
            break;
        }
        case POSITION:
            result = ValueInt.get(locate(v0.getString(), v1.getString(), 0));
            break;
        case XMLATTR:
            result = ValueString.get(StringUtils.xmlAttr(v0.getString(), v1.getString()));
            break;
        case XMLNODE: {
            String attr = v1 == null ? null : v1 == ValueNull.INSTANCE ? null : v1.getString();
            String content = v2 == null ? null : v2 == ValueNull.INSTANCE ? null : v2.getString();
            boolean indent = v3 == null ? true : v3.getBoolean();
            result = ValueString.get(StringUtils.xmlNode(v0.getString(), attr, content, indent));
            break;
        }
        case REGEXP_REPLACE: {
            String regexp = v1.getString();
            try {
                result = ValueString.get(v0.getString().replaceAll(regexp, v2.getString()));
            } catch (PatternSyntaxException e) {
                throw DbException.get(ErrorCode.LIKE_ESCAPE_ERROR_1, e, regexp);
            }
            break;
        }
        case RPAD:
            result = ValueString
                    .get(StringUtils.pad(v0.getString(), v1.getInt(), v2 == null ? null : v2.getString(), true));
            break;
        case LPAD:
            result = ValueString
                    .get(StringUtils.pad(v0.getString(), v1.getInt(), v2 == null ? null : v2.getString(), false));
            break;
        case XMLTEXT:
            if (v1 == null) {
                result = ValueString.get(StringUtils.xmlText(v0.getString()));
            } else {
                result = ValueString.get(StringUtils.xmlText(v0.getString(), v1.getBoolean()));
            }
            break;
        case CONCAT_WS: // 表示: concat with separator
        case CONCAT: {
            result = ValueNull.INSTANCE;
            int start = 0;
            String separator = "";
            if (info.type == CONCAT_WS) {
                start = 1;
                separator = getNullOrValue(session, args, values, 0).getString();
            }
            for (int i = start; i < args.length; i++) {
                Value v = getNullOrValue(session, args, values, i);
                if (v == ValueNull.INSTANCE) {
                    continue;
                }
                if (result == ValueNull.INSTANCE) {
                    result = v;
                } else {
                    String tmp = v.getString();
                    if (!StringUtils.isNullOrEmpty(separator) && !StringUtils.isNullOrEmpty(tmp)) {
                        tmp = separator.concat(tmp);
                    }
                    result = ValueString.get(result.getString().concat(tmp));
                }
            }
            if (info.type == CONCAT_WS) {
                if (separator != null && result == ValueNull.INSTANCE) {
                    result = ValueString.get("");
                }
            }
            break;
        }
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private static int getDifference(String s1, String s2) {
        // TODO function difference: compatibility with SQL Server and HSQLDB
        s1 = getSoundex(s1);
        s2 = getSoundex(s2);
        int e = 0;
        for (int i = 0; i < 4; i++) {
            if (s1.charAt(i) == s2.charAt(i)) {
                e++;
            }
        }
        return e;
    }

    private static String insert(String s1, int start, int length, String s2) {
        if (s1 == null) {
            return s2;
        }
        if (s2 == null) {
            return s1;
        }
        int len1 = s1.length();
        int len2 = s2.length();
        start--;
        if (start < 0 || length <= 0 || len2 == 0 || start > len1) {
            return s1;
        }
        if (start + length > len1) {
            length = len1 - start;
        }
        return s1.substring(0, start) + s2 + s1.substring(start + length);
    }

    private static String left(String s, int count) {
        if (count < 0) {
            count = 0;
        } else if (count > s.length()) {
            count = s.length();
        }
        return s.substring(0, count);
    }

    private static String right(String s, int count) {
        if (count < 0) {
            count = 0;
        } else if (count > s.length()) {
            count = s.length();
        }
        return s.substring(s.length() - count);
    }

    private static int locate(String search, String s, int start) {
        if (start < 0) {
            int i = s.length() + start;
            return s.lastIndexOf(search, i) + 1;
        }
        int i = (start == 0) ? 0 : start - 1;
        return s.indexOf(search, i) + 1;
    }

    private static String repeat(String s, int count) {
        StringBuilder buff = new StringBuilder(s.length() * count);
        while (count-- > 0) {
            buff.append(s);
        }
        return buff.toString();
    }

    private static String replace(String s, String replace, String with) {
        if (s == null || replace == null || with == null) {
            return null;
        }
        if (replace.length() == 0) {
            // avoid out of memory
            return s;
        }
        StringBuilder buff = new StringBuilder(s.length());
        int start = 0;
        int len = replace.length();
        while (true) {
            int i = s.indexOf(replace, start);
            if (i == -1) {
                break;
            }
            buff.append(s.substring(start, i)).append(with);
            start = i + len;
        }
        buff.append(s.substring(start));
        return buff.toString();
    }

    private static String substring(String s, int start, int length) {
        int len = s.length();
        start--;
        if (start < 0) {
            start = 0;
        }
        if (length < 0) {
            length = 0;
        }
        start = (start > len) ? len : start;
        if (start + length > len) {
            length = len - start;
        }
        return s.substring(start, start + length);
    }

    @Override
    protected void checkParameterCount(int len) {
        int min = 0, max = Integer.MAX_VALUE;
        switch (info.type) {
        case LTRIM:
        case RTRIM:
        case TRIM:
        case XMLTEXT:
            min = 1;
            max = 2;
            break;
        case REPLACE:
        case LOCATE:
        case INSTR:
        case SUBSTR:
        case SUBSTRING:
        case LPAD:
        case RPAD:
            min = 2;
            max = 3;
            break;
        case CONCAT:
        case CONCAT_WS:
            min = 2;
            break;
        case XMLNODE:
            min = 1;
            max = 4;
            break;
        default:
            DbException.throwInternalError("type=" + info.type);
        }
        checkParameterCount(len, min, max);
    }

    @Override
    public Expression optimize(ServerSession session) {
        boolean allConst = optimizeArgs(session);
        int t, s, d;
        long p;
        switch (info.type) {
        case SUBSTRING:
        case SUBSTR: {
            t = info.dataType;
            p = args[0].getPrecision();
            s = 0;
            if (args[1].isConstant()) {
                // if only two arguments are used,
                // subtract offset from first argument length
                p -= args[1].getValue(session).getLong() - 1;
            }
            if (args.length == 3 && args[2].isConstant()) {
                // if the third argument is constant it is at most this value
                p = Math.min(p, args[2].getValue(session).getLong());
            }
            p = Math.max(0, p);
            d = MathUtils.convertLongToInt(p);
            break;
        }
        default:
            t = info.dataType;
            DataType type = DataType.getDataType(t);
            p = PRECISION_UNKNOWN;
            d = 0;
            s = type.defaultScale;
        }
        dataType = t;
        precision = p;
        scale = s;
        displaySize = d;
        if (allConst) {
            Value v = getValue(session);
            return ValueExpression.get(v);
        }
        return this;
    }

    @Override
    protected void calculatePrecisionAndDisplaySize() {
        switch (info.type) {
        case CHAR:
            precision = 1;
            displaySize = 1;
            break;
        case CONCAT:
            precision = 0;
            displaySize = 0;
            for (Expression e : args) {
                precision += e.getPrecision();
                displaySize = MathUtils.convertLongToInt((long) displaySize + e.getDisplaySize());
                if (precision < 0) {
                    precision = Long.MAX_VALUE;
                }
            }
            break;
        case HEXTORAW:
            precision = (args[0].getPrecision() + 3) / 4;
            displaySize = MathUtils.convertLongToInt(precision);
            break;
        case LCASE:
        case LTRIM:
        case RIGHT:
        case RTRIM:
        case UCASE:
        case LOWER:
        case UPPER:
        case TRIM:
        case STRINGDECODE:
        case UTF8TOSTRING:
            precision = args[0].getPrecision();
            displaySize = args[0].getDisplaySize();
            break;
        case RAWTOHEX:
            precision = args[0].getPrecision() * 4;
            displaySize = MathUtils.convertLongToInt(precision);
            break;
        case SOUNDEX:
            precision = 4;
            displaySize = (int) precision;
            break;
        default:
            super.calculatePrecisionAndDisplaySize();
        }
    }
}
