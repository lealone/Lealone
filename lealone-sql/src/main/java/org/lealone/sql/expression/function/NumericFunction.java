/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import java.util.Calendar;

import org.lealone.common.compress.CompressTool;
import org.lealone.common.exceptions.DbException;
import org.lealone.common.security.BlockCipher;
import org.lealone.common.security.CipherFactory;
import org.lealone.common.security.SHA256;
import org.lealone.common.util.DataUtils;
import org.lealone.common.util.MathUtils;
import org.lealone.db.Database;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.session.ServerSession;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueBytes;
import org.lealone.db.value.ValueDate;
import org.lealone.db.value.ValueDouble;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueTimestamp;
import org.lealone.db.value.ValueUuid;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ValueExpression;

/**
 * Implementation of the numeric functions.
 * 
 * @author H2 Group
 * @author zhh
 */
public class NumericFunction extends BuiltInFunction {

    public static void init() {
    }

    public static final int ABS = 0, ACOS = 1, ASIN = 2, ATAN = 3, ATAN2 = 4, BITAND = 5, BITOR = 6,
            BITXOR = 7, CEILING = 8, COS = 9, COT = 10, DEGREES = 11, EXP = 12, FLOOR = 13, LOG = 14,
            LOG10 = 15, MOD = 16, PI = 17, POWER = 18, RADIANS = 19, RAND = 20, ROUND = 21,
            ROUNDMAGIC = 22, SIGN = 23, SIN = 24, SQRT = 25, TAN = 26, TRUNCATE = 27, SECURE_RAND = 28,
            HASH = 29, ENCRYPT = 30, DECRYPT = 31, COMPRESS = 32, EXPAND = 33, ZERO = 34,
            RANDOM_UUID = 35, COSH = 36, SINH = 37, TANH = 38, LN = 39;

    static {
        addFunction("ABS", ABS, 1, Value.NULL);
        addFunction("ACOS", ACOS, 1, Value.DOUBLE);
        addFunction("ASIN", ASIN, 1, Value.DOUBLE);
        addFunction("ATAN", ATAN, 1, Value.DOUBLE);
        addFunction("ATAN2", ATAN2, 2, Value.DOUBLE);
        addFunction("BITAND", BITAND, 2, Value.LONG);
        addFunction("BITOR", BITOR, 2, Value.LONG);
        addFunction("BITXOR", BITXOR, 2, Value.LONG);
        addFunction("CEILING", CEILING, 1, Value.DOUBLE);
        addFunction("CEIL", CEILING, 1, Value.DOUBLE);
        addFunction("COS", COS, 1, Value.DOUBLE);
        addFunction("COSH", COSH, 1, Value.DOUBLE);
        addFunction("COT", COT, 1, Value.DOUBLE);
        addFunction("DEGREES", DEGREES, 1, Value.DOUBLE);
        addFunction("EXP", EXP, 1, Value.DOUBLE);
        addFunction("FLOOR", FLOOR, 1, Value.DOUBLE);
        addFunction("LOG", LOG, 1, Value.DOUBLE);
        addFunction("LN", LN, 1, Value.DOUBLE);
        addFunction("LOG10", LOG10, 1, Value.DOUBLE);
        addFunction("MOD", MOD, 2, Value.LONG);
        addFunction("PI", PI, 0, Value.DOUBLE);
        addFunction("POWER", POWER, 2, Value.DOUBLE);
        addFunction("RADIANS", RADIANS, 1, Value.DOUBLE);
        // RAND without argument: get the next value
        // RAND with one argument: seed the random generator
        addFunctionNotDeterministic("RAND", RAND, VAR_ARGS, Value.DOUBLE);
        addFunctionNotDeterministic("RANDOM", RAND, VAR_ARGS, Value.DOUBLE);
        addFunction("ROUND", ROUND, VAR_ARGS, Value.DOUBLE);
        addFunction("ROUNDMAGIC", ROUNDMAGIC, 1, Value.DOUBLE);
        addFunction("SIGN", SIGN, 1, Value.INT);
        addFunction("SIN", SIN, 1, Value.DOUBLE);
        addFunction("SINH", SINH, 1, Value.DOUBLE);
        addFunction("SQRT", SQRT, 1, Value.DOUBLE);
        addFunction("TAN", TAN, 1, Value.DOUBLE);
        addFunction("TANH", TANH, 1, Value.DOUBLE);
        addFunction("TRUNCATE", TRUNCATE, VAR_ARGS, Value.NULL);
        // same as TRUNCATE
        addFunction("TRUNC", TRUNCATE, VAR_ARGS, Value.NULL);
        addFunction("HASH", HASH, 3, Value.BYTES);
        addFunction("ENCRYPT", ENCRYPT, 3, Value.BYTES);
        addFunction("DECRYPT", DECRYPT, 3, Value.BYTES);
        addFunctionNotDeterministic("SECURE_RAND", SECURE_RAND, 1, Value.BYTES);
        addFunction("COMPRESS", COMPRESS, VAR_ARGS, Value.BYTES);
        addFunction("EXPAND", EXPAND, 1, Value.BYTES);
        addFunction("ZERO", ZERO, 0, Value.INT);
        addFunctionNotDeterministic("RANDOM_UUID", RANDOM_UUID, 0, Value.UUID);
        addFunctionNotDeterministic("SYS_GUID", RANDOM_UUID, 0, Value.UUID);
    }

    protected NumericFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected Value getValue0(ServerSession session) {
        Value result;
        switch (info.type) {
        case PI:
            result = ValueDouble.get(Math.PI);
            break;
        case ZERO:
            result = ValueInt.get(0);
            break;
        case RANDOM_UUID:
            result = ValueUuid.getNewRandom();
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
        case ABS:
            result = v.getSignum() > 0 ? v : v.negate();
            break;
        case ACOS:
            result = ValueDouble.get(Math.acos(v.getDouble()));
            break;
        case ASIN:
            result = ValueDouble.get(Math.asin(v.getDouble()));
            break;
        case ATAN:
            result = ValueDouble.get(Math.atan(v.getDouble()));
            break;
        case CEILING:
            result = ValueDouble.get(Math.ceil(v.getDouble()));
            break;
        case COS:
            result = ValueDouble.get(Math.cos(v.getDouble()));
            break;
        case COSH:
            result = ValueDouble.get(Math.cosh(v.getDouble()));
            break;
        case COT: {
            double d = Math.tan(v.getDouble());
            if (d == 0.0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
            }
            result = ValueDouble.get(1. / d);
            break;
        }
        case DEGREES:
            result = ValueDouble.get(Math.toDegrees(v.getDouble()));
            break;
        case EXP:
            result = ValueDouble.get(Math.exp(v.getDouble()));
            break;
        case FLOOR:
            result = ValueDouble.get(Math.floor(v.getDouble()));
            break;
        case LN: // 底数e的多少次幂是v0
            result = ValueDouble.get(Math.log(v.getDouble()));
            break;
        case LOG: // 底数10的多少次幂是v0
            if (database.getMode().logIsLogBase10) {
                result = ValueDouble.get(Math.log10(v.getDouble()));
            } else {
                result = ValueDouble.get(Math.log(v.getDouble()));
            }
            break;
        case LOG10:
            result = ValueDouble.get(log10(v.getDouble()));
            break;
        case RADIANS:
            result = ValueDouble.get(Math.toRadians(v.getDouble()));
            break;
        case ROUNDMAGIC:
            result = ValueDouble.get(roundmagic(v.getDouble()));
            break;
        case SIGN:
            result = ValueInt.get(v.getSignum());
            break;
        case SIN:
            result = ValueDouble.get(Math.sin(v.getDouble()));
            break;
        case SINH:
            result = ValueDouble.get(Math.sinh(v.getDouble()));
            break;
        case SQRT:
            result = ValueDouble.get(Math.sqrt(v.getDouble()));
            break;
        case TAN:
            result = ValueDouble.get(Math.tan(v.getDouble()));
            break;
        case TANH:
            result = ValueDouble.get(Math.tanh(v.getDouble()));
            break;
        case SECURE_RAND:
            result = ValueBytes.getNoCopy(MathUtils.secureRandomBytes(v.getInt()));
            break;
        case EXPAND: // 解压，对应COMPRESS函数
            result = ValueBytes.getNoCopy(CompressTool.getInstance().expand(v.getBytesNoCopy()));
            break;
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private static strictfp double log10(double value) {
        return roundmagic(StrictMath.log(value) / StrictMath.log(10));
    }

    private static double roundmagic(double d) {
        if ((d < 0.0000000000001) && (d > -0.0000000000001)) {
            return 0.0;
        }
        if ((d > 1000000000000.) || (d < -1000000000000.)) {
            return d;
        }
        StringBuilder s = new StringBuilder();
        s.append(d);
        if (s.toString().indexOf("E") >= 0) {
            return d;
        }
        int len = s.length();
        if (len < 16) {
            return d;
        }
        if (s.toString().indexOf(".") > len - 3) {
            return d;
        }
        s.delete(len - 2, len);
        len -= 2;
        char c1 = s.charAt(len - 2);
        char c2 = s.charAt(len - 3);
        char c3 = s.charAt(len - 4);
        if ((c1 == '0') && (c2 == '0') && (c3 == '0')) {
            s.setCharAt(len - 1, '0');
        } else if ((c1 == '9') && (c2 == '9') && (c3 == '9')) {
            s.setCharAt(len - 1, '9');
            s.append('9');
            s.append('9');
            s.append('9');
        }
        return Double.parseDouble(s.toString());
    }

    @Override
    protected Value getValueN(ServerSession session, Expression[] args, Value[] values) {
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value v2 = getNullOrValue(session, args, values, 2);
        Value result;
        switch (info.type) {
        case ATAN2:
            result = ValueDouble.get(Math.atan2(v0.getDouble(), v1.getDouble()));
            break;
        case BITAND:
            result = ValueLong.get(v0.getLong() & v1.getLong());
            break;
        case BITOR:
            result = ValueLong.get(v0.getLong() | v1.getLong());
            break;
        case BITXOR:
            result = ValueLong.get(v0.getLong() ^ v1.getLong());
            break;
        case MOD: {
            long x = v1.getLong();
            if (x == 0) {
                throw DbException.get(ErrorCode.DIVISION_BY_ZERO_1, getSQL());
            }
            result = ValueLong.get(v0.getLong() % x);
            break;
        }
        case POWER:
            result = ValueDouble.get(Math.pow(v0.getDouble(), v1.getDouble()));
            break;
        case RAND: {
            if (v0 != null) {
                session.getRandom().setSeed(v0.getInt());
            }
            result = ValueDouble.get(session.getRandom().nextDouble());
            break;
        }
        case ROUND: {
            // sql = "SELECT ROUND(12.234, 2)"; //12.23, 4舍5入，小数保留两位
            // sql = "SELECT ROUND(12.235, 2)"; //12.24
            // sql = "SELECT ROUND(12.236, 2)"; //12.24
            double f = v1 == null ? 1. : Math.pow(10., v1.getDouble()); // 小数位是2，相当于先求10的2次方
            result = ValueDouble.get(Math.round(v0.getDouble() * f) / f);
            break;
        }
        case TRUNCATE: {
            if (v0.getType() == Value.TIMESTAMP) {
                java.sql.Timestamp d = v0.getTimestamp();
                Calendar c = Calendar.getInstance();
                c.setTime(d);
                c.set(Calendar.HOUR, 0);
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);
                result = ValueTimestamp.get(new java.sql.Timestamp(c.getTimeInMillis()));
            } else {
                double d = v0.getDouble();
                int p = v1.getInt();
                double f = Math.pow(10., p);
                double g = d * f;
                result = ValueDouble.get(((d < 0) ? Math.ceil(g) : Math.floor(g)) / f);
            }
            break;
        }
        case HASH:
            result = ValueBytes.getNoCopy(getHash(v0.getString(), v1.getBytesNoCopy(), v2.getInt()));
            break;
        case ENCRYPT:
            result = ValueBytes
                    .getNoCopy(encrypt(v0.getString(), v1.getBytesNoCopy(), v2.getBytesNoCopy()));
            break;
        case DECRYPT:
            result = ValueBytes
                    .getNoCopy(decrypt(v0.getString(), v1.getBytesNoCopy(), v2.getBytesNoCopy()));
            break;
        case COMPRESS: {
            String algorithm = null;
            if (v1 != null) {
                algorithm = v1.getString();
            }
            result = ValueBytes
                    .getNoCopy(CompressTool.getInstance().compress(v0.getBytesNoCopy(), algorithm));
            break;
        }
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private static byte[] getHash(String algorithm, byte[] bytes, int iterations) {
        if (!"SHA256".equalsIgnoreCase(algorithm)) {
            throw DbException.getInvalidValueException("algorithm", algorithm);
        }
        for (int i = 0; i < iterations; i++) {
            bytes = SHA256.getHash(bytes, false);
        }
        return bytes;
    }

    private static byte[] encrypt(String algorithm, byte[] key, byte[] data) {
        BlockCipher cipher = CipherFactory.getBlockCipher(algorithm);
        byte[] newKey = getPaddedArrayCopy(key, cipher.getKeyLength());
        cipher.setKey(newKey);
        byte[] newData = getPaddedArrayCopy(data, BlockCipher.ALIGN);
        cipher.encrypt(newData, 0, newData.length);
        return newData;
    }

    private static byte[] decrypt(String algorithm, byte[] key, byte[] data) {
        BlockCipher cipher = CipherFactory.getBlockCipher(algorithm);
        byte[] newKey = getPaddedArrayCopy(key, cipher.getKeyLength());
        cipher.setKey(newKey);
        byte[] newData = getPaddedArrayCopy(data, BlockCipher.ALIGN);
        cipher.decrypt(newData, 0, newData.length);
        return newData;
    }

    private static byte[] getPaddedArrayCopy(byte[] data, int blockSize) {
        int size = MathUtils.roundUpInt(data.length, blockSize);
        byte[] newData = DataUtils.newBytes(size);
        System.arraycopy(data, 0, newData, 0, data.length);
        return newData;
    }

    @Override
    protected void checkParameterCount(int len) {
        int min = 0, max = Integer.MAX_VALUE;
        switch (info.type) {
        case RAND:
            max = 1;
            break;
        case COMPRESS:
        case ROUND:
        case TRUNCATE:
            min = 1;
            max = 2;
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
        Expression p0 = args.length < 1 ? null : args[0];
        switch (info.type) {
        case TRUNCATE:
            t = p0.getType();
            s = p0.getScale();
            p = p0.getPrecision();
            d = p0.getDisplaySize();
            if (t == Value.NULL) {
                t = Value.INT;
                p = ValueInt.PRECISION;
                d = ValueInt.DISPLAY_SIZE;
                s = 0;
            } else if (t == Value.TIMESTAMP) {
                t = Value.DATE;
                p = ValueDate.PRECISION;
                s = 0;
                d = ValueDate.DISPLAY_SIZE;
            }
            break;
        case ABS:
        case FLOOR:
        case RADIANS:
        case ROUND:
        case POWER:
            t = p0.getType();
            s = p0.getScale();
            p = p0.getPrecision();
            d = p0.getDisplaySize();
            if (t == Value.NULL) {
                t = Value.INT;
                p = ValueInt.PRECISION;
                d = ValueInt.DISPLAY_SIZE;
                s = 0;
            }
            break;
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
        case ENCRYPT:
        case DECRYPT:
            precision = args[2].getPrecision();
            displaySize = args[2].getDisplaySize();
            break;
        case COMPRESS:
            precision = args[0].getPrecision();
            displaySize = args[0].getDisplaySize();
            break;
        case TRUNCATE:
            precision = args[0].getPrecision();
            displaySize = args[0].getDisplaySize();
            break;
        default:
            super.calculatePrecisionAndDisplaySize();
        }
    }
}
