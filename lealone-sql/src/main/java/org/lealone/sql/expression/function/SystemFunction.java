/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.lealone.sql.expression.function;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.JdbcUtils;
import org.lealone.common.util.StatementBuilder;
import org.lealone.common.util.StringUtils;
import org.lealone.common.util.Utils;
import org.lealone.db.Command;
import org.lealone.db.Constants;
import org.lealone.db.Database;
import org.lealone.db.Mode;
import org.lealone.db.api.ErrorCode;
import org.lealone.db.schema.Schema;
import org.lealone.db.schema.Sequence;
import org.lealone.db.session.ServerSession;
import org.lealone.db.table.Column;
import org.lealone.db.table.Table;
import org.lealone.db.util.Csv;
import org.lealone.db.value.DataType;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueArray;
import org.lealone.db.value.ValueBoolean;
import org.lealone.db.value.ValueInt;
import org.lealone.db.value.ValueLong;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueResultSet;
import org.lealone.db.value.ValueString;
import org.lealone.sql.LealoneSQLParser;
import org.lealone.sql.expression.Expression;
import org.lealone.sql.expression.ExpressionColumn;
import org.lealone.sql.expression.SequenceValue;
import org.lealone.sql.expression.ValueExpression;
import org.lealone.sql.expression.Variable;
import org.lealone.storage.fs.FileUtils;

/**
 * Implementation of the system functions.
 * 
 * @author H2 Group
 * @author zhh
 */
public class SystemFunction extends BuiltInFunction {

    public static final int DATABASE = 150, USER = 151, CURRENT_USER = 152, IDENTITY = 153,
            SCOPE_IDENTITY = 154, AUTOCOMMIT = 155, READONLY = 156, DATABASE_PATH = 157,
            LOCK_TIMEOUT = 158, DISK_SPACE_USED = 159;

    public static final int IFNULL = 200, CASEWHEN = 201, CONVERT = 202, CAST = 203, COALESCE = 204,
            NULLIF = 205, CASE = 206, NEXTVAL = 207, CURRVAL = 208, ARRAY_GET = 209, CSVREAD = 210,
            CSVWRITE = 211, MEMORY_FREE = 212, MEMORY_USED = 213, TRANSACTION_ISOLATION_LEVEL = 214,
            SCHEMA = 215, SESSION_ID = 216, ARRAY_LENGTH = 217, GREATEST = 218, LEAST = 219,
            CANCEL_SESSION = 220, SET = 221, FILE_READ = 222, TRANSACTION_ID = 223, TRUNCATE_VALUE = 224,
            NVL2 = 225, DECODE = 226, ARRAY_CONTAINS = 227;

    /**
     * This is called LEALONE_VERSION() and not VERSION(), because we return a fake value
     * for VERSION() when running under the PostgreSQL ODBC driver.
     */
    public static final int LEALONE_VERSION = 229;
    public static final int ROW_NUMBER = 230;

    public static void init() {
        addFunctionNotDeterministic("DATABASE", DATABASE, 0, Value.STRING);
        addFunctionNotDeterministic("USER", USER, 0, Value.STRING);
        addFunctionNotDeterministic("CURRENT_USER", CURRENT_USER, 0, Value.STRING);
        addFunctionNotDeterministic("IDENTITY", IDENTITY, 0, Value.LONG);
        addFunctionNotDeterministic("SCOPE_IDENTITY", SCOPE_IDENTITY, 0, Value.LONG);
        addFunctionNotDeterministic("IDENTITY_VAL_LOCAL", IDENTITY, 0, Value.LONG);
        addFunctionNotDeterministic("LAST_INSERT_ID", IDENTITY, 0, Value.LONG);
        addFunctionNotDeterministic("LASTVAL", IDENTITY, 0, Value.LONG);
        addFunctionNotDeterministic("AUTOCOMMIT", AUTOCOMMIT, 0, Value.BOOLEAN);
        addFunctionNotDeterministic("READONLY", READONLY, 0, Value.BOOLEAN);
        addFunction("DATABASE_PATH", DATABASE_PATH, 0, Value.STRING);
        addFunctionNotDeterministic("LOCK_TIMEOUT", LOCK_TIMEOUT, 0, Value.INT);
        addFunctionWithNull("IFNULL", IFNULL, 2, Value.NULL);
        addFunctionWithNull("ISNULL", IFNULL, 2, Value.NULL);
        addFunctionWithNull("CASEWHEN", CASEWHEN, 3, Value.NULL);
        addFunctionWithNull("CONVERT", CONVERT, 1, Value.NULL);
        addFunctionWithNull("CAST", CAST, 1, Value.NULL);
        addFunctionWithNull("TRUNCATE_VALUE", TRUNCATE_VALUE, 3, Value.NULL);
        addFunctionWithNull("COALESCE", COALESCE, VAR_ARGS, Value.NULL);
        addFunctionWithNull("NVL", COALESCE, VAR_ARGS, Value.NULL);
        addFunctionWithNull("NVL2", NVL2, 3, Value.NULL);
        addFunctionWithNull("NULLIF", NULLIF, 2, Value.NULL);
        addFunctionWithNull("CASE", CASE, VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("NEXTVAL", NEXTVAL, VAR_ARGS, Value.LONG);
        addFunctionNotDeterministic("CURRVAL", CURRVAL, VAR_ARGS, Value.LONG);
        addFunction("ARRAY_GET", ARRAY_GET, 2, Value.STRING);
        addFunction("ARRAY_CONTAINS", ARRAY_CONTAINS, 2, Value.BOOLEAN, false, true);
        addFunction("CSVREAD", CSVREAD, VAR_ARGS, Value.RESULT_SET, false, false);
        addFunction("CSVWRITE", CSVWRITE, VAR_ARGS, Value.INT, false, false);
        addFunctionNotDeterministic("MEMORY_FREE", MEMORY_FREE, 0, Value.INT);
        addFunctionNotDeterministic("MEMORY_USED", MEMORY_USED, 0, Value.INT);
        addFunctionNotDeterministic("TRANSACTION_ISOLATION_LEVEL", TRANSACTION_ISOLATION_LEVEL, 0,
                Value.INT);
        addFunctionNotDeterministic("SCHEMA", SCHEMA, 0, Value.STRING);
        addFunctionNotDeterministic("SESSION_ID", SESSION_ID, 0, Value.INT);
        addFunction("ARRAY_LENGTH", ARRAY_LENGTH, 1, Value.INT);
        addFunctionWithNull("LEAST", LEAST, VAR_ARGS, Value.NULL);
        addFunctionWithNull("GREATEST", GREATEST, VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("CANCEL_SESSION", CANCEL_SESSION, 1, Value.BOOLEAN);
        addFunction("SET", SET, 2, Value.NULL, false, false);
        addFunction("FILE_READ", FILE_READ, VAR_ARGS, Value.NULL, false, false);
        addFunctionNotDeterministic("TRANSACTION_ID", TRANSACTION_ID, 0, Value.STRING);
        addFunctionWithNull("DECODE", DECODE, VAR_ARGS, Value.NULL);
        addFunctionNotDeterministic("DISK_SPACE_USED", DISK_SPACE_USED, 1, Value.LONG);
        addFunction("LEALONE_VERSION", LEALONE_VERSION, 0, Value.STRING);

        // pseudo function
        addFunctionWithNull("ROW_NUMBER", ROW_NUMBER, 0, Value.LONG);
    }

    protected SystemFunction(Database database, FunctionInfo info) {
        super(database, info);
    }

    @Override
    protected Value getValue0(ServerSession session) {
        Value result;
        switch (info.type) {
        case DATABASE:
            result = ValueString.get(database.getShortName());
            break;
        case USER:
        case CURRENT_USER:
            result = ValueString.get(session.getUser().getName());
            break;
        case IDENTITY:
            result = session.getLastIdentity();
            break;
        case SCOPE_IDENTITY:
            result = session.getLastScopeIdentity();
            break;
        case AUTOCOMMIT:
            result = ValueBoolean.get(session.isAutoCommit());
            break;
        case READONLY:
            result = ValueBoolean.get(database.isReadOnly());
            break;
        case DATABASE_PATH: {
            String path = database.getDatabasePath();
            result = path == null ? (Value) ValueNull.INSTANCE : ValueString.get(path);
            break;
        }
        case LOCK_TIMEOUT:
            result = ValueInt.get(session.getLockTimeout());
            break;
        case MEMORY_FREE:
            session.getUser().checkAdmin();
            result = ValueInt.get(Utils.getMemoryFree());
            break;
        case MEMORY_USED:
            session.getUser().checkAdmin();
            result = ValueInt.get(Utils.getMemoryUsed());
            break;
        case TRANSACTION_ISOLATION_LEVEL:
            result = ValueInt.get(session.getTransactionIsolationLevel());
            break;
        case SCHEMA:
            result = ValueString.get(session.getCurrentSchemaName());
            break;
        case SESSION_ID:
            result = ValueInt.get(session.getId());
            break;
        case TRANSACTION_ID: {
            result = session.getTransactionId();
            break;
        }
        case LEALONE_VERSION:
            result = ValueString.get(Constants.getVersion());
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
        case DISK_SPACE_USED:
            result = ValueLong.get(getDiskSpaceUsed(session, v));
            break;
        case CAST:
        case CONVERT: {
            v = v.convertTo(dataType);
            Mode mode = database.getMode();
            v = v.convertScale(mode.convertOnlyToSmallerScale, scale);
            v = v.convertPrecision(getPrecision(), false);
            result = v;
            break;
        }
        case ARRAY_LENGTH: {
            if (v.getType() == Value.ARRAY) {
                Value[] list = ((ValueArray) v).getList();
                result = ValueInt.get(list.length);
            } else {
                result = ValueNull.INSTANCE;
            }
            break;
        }
        case CANCEL_SESSION: {
            result = ValueBoolean.get(cancelStatement(session, v.getInt()));
            break;
        }
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private static long getDiskSpaceUsed(ServerSession session, Value v0) {
        LealoneSQLParser p = new LealoneSQLParser(session);
        String sql = v0.getString();
        Table table = p.parseTableName(sql);
        return table.getDiskSpaceUsed();
    }

    private static boolean cancelStatement(ServerSession session, int targetSessionId) {
        session.getUser().checkAdmin();
        ServerSession[] sessions = session.getDatabase().getSessions(false);
        for (ServerSession s : sessions) {
            if (s.getId() == targetSessionId) {
                Command c = s.getCurrentCommand();
                if (c == null) {
                    return false;
                }
                c.cancel();
                return true;
            }
        }
        return false;
    }

    @Override
    protected Value getValueN(ServerSession session, Expression[] args, Value[] values) {
        Value v0 = getNullOrValue(session, args, values, 0);
        Value v1 = getNullOrValue(session, args, values, 1);
        Value v2 = getNullOrValue(session, args, values, 2);
        Value v3 = getNullOrValue(session, args, values, 3);
        Value v4 = getNullOrValue(session, args, values, 4);
        Value v5 = getNullOrValue(session, args, values, 5);
        Value result;
        switch (info.type) {
        case NULLIF:
            // 相等返回null，不相等返回v0
            result = database.areEqual(v0, v1) ? ValueNull.INSTANCE : v0;
            break;
        case NEXTVAL: {
            Sequence sequence = getSequence(session, v0, v1);
            SequenceValue value = new SequenceValue(sequence);
            result = value.getValue(session);
            break;
        }
        case CURRVAL: {
            Sequence sequence = getSequence(session, v0, v1);
            result = ValueLong.get(sequence.getCurrentValue(session));
            break;
        }
        case CSVREAD: {
            String fileName = v0.getString();
            String columnList = v1 == null ? null : v1.getString();
            Csv csv = new Csv();
            String options = v2 == null ? null : v2.getString();
            String charset = null;
            if (options != null && options.indexOf('=') >= 0) {
                charset = csv.setOptions(options);
            } else {
                charset = options;
                String fieldSeparatorRead = v3 == null ? null : v3.getString();
                String fieldDelimiter = v4 == null ? null : v4.getString();
                String escapeCharacter = v5 == null ? null : v5.getString();
                Value v6 = getNullOrValue(session, args, values, 6);
                String nullString = v6 == null ? null : v6.getString();
                setCsvDelimiterEscape(csv, fieldSeparatorRead, fieldDelimiter, escapeCharacter);
                csv.setNullString(nullString);
            }
            char fieldSeparator = csv.getFieldSeparatorRead();
            String[] columns = StringUtils.arraySplit(columnList, fieldSeparator);
            try {
                ValueResultSet vr = ValueResultSet.get(csv.read(fileName, columns, charset));
                result = vr;
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            break;
        }
        case CSVWRITE: {
            session.getUser().checkAdmin();
            Connection conn = session.createConnection(false);
            Csv csv = new Csv();
            String options = v2 == null ? null : v2.getString();
            String charset = null;
            if (options != null && options.indexOf('=') >= 0) {
                charset = csv.setOptions(options);
            } else {
                charset = options;
                String fieldSeparatorWrite = v3 == null ? null : v3.getString();
                String fieldDelimiter = v4 == null ? null : v4.getString();
                String escapeCharacter = v5 == null ? null : v5.getString();
                Value v6 = getNullOrValue(session, args, values, 6);
                String nullString = v6 == null ? null : v6.getString();
                Value v7 = getNullOrValue(session, args, values, 7);
                String lineSeparator = v7 == null ? null : v7.getString();
                setCsvDelimiterEscape(csv, fieldSeparatorWrite, fieldDelimiter, escapeCharacter);
                csv.setNullString(nullString);
                if (lineSeparator != null) {
                    csv.setLineSeparator(lineSeparator);
                }
            }
            try {
                int rows = csv.write(conn, v0.getString(), v1.getString(), charset);
                result = ValueInt.get(rows);
            } catch (SQLException e) {
                throw DbException.convert(e);
            }
            break;
        }
        case SET: {
            Variable var = (Variable) args[0];
            session.setVariable(var.getName(), v1);
            result = v1;
            break;
        }
        case FILE_READ: {
            session.getUser().checkAdmin();
            String fileName = v0.getString();
            boolean blob = args.length == 1;
            try {
                try (InputStream in = FileUtils.newInputStream(fileName)) {
                    if (blob) {
                        result = session.getDataHandler().getLobStorage().createBlob(in, -1);
                    } else {
                        Reader reader;
                        if (v1 == ValueNull.INSTANCE) {
                            reader = new InputStreamReader(in);
                        } else {
                            reader = new InputStreamReader(in, v1.getString());
                        }
                        result = session.getDataHandler().getLobStorage().createClob(reader, -1);
                    }
                }
            } catch (IOException e) {
                throw DbException.convertIOException(e, fileName);
            }
            break;
        }
        case TRUNCATE_VALUE: {
            result = v0.convertPrecision(v1.getLong(), v2.getBoolean());
            break;
        }
        case IFNULL: {
            result = v0;
            if (v0 == ValueNull.INSTANCE) {
                result = getNullOrValue(session, args, values, 1);
            }
            break;
        }
        case CASEWHEN: {
            Value v;
            if (v0 == ValueNull.INSTANCE || !v0.getBoolean()) {
                v = getNullOrValue(session, args, values, 2);
            } else {
                v = getNullOrValue(session, args, values, 1);
            }
            result = v.convertTo(dataType);
            break;
        }
        case DECODE: {
            int index = -1;
            for (int i = 1; i < args.length - 1; i += 2) {
                if (database.areEqual(v0, getNullOrValue(session, args, values, i))) {
                    index = i + 1;
                    break; // 要加break
                }
            }
            if (index < 0 && args.length % 2 == 0) {
                index = args.length - 1;
            }
            Value v = index < 0 ? ValueNull.INSTANCE : getNullOrValue(session, args, values, index);
            result = v.convertTo(dataType);
            break;
        }
        case NVL2: {
            Value v;
            if (v0 == ValueNull.INSTANCE) {
                v = getNullOrValue(session, args, values, 2);
            } else {
                v = getNullOrValue(session, args, values, 1);
            }
            result = v.convertTo(dataType);
            break;
        }
        case COALESCE: { // 返回第一个不为null的
            result = v0;
            for (int i = 0; i < args.length; i++) {
                Value v = getNullOrValue(session, args, values, i);
                if (!(v == ValueNull.INSTANCE)) {
                    result = v.convertTo(dataType);
                    break;
                }
            }
            break;
        }
        case GREATEST: // 最大的一个
        case LEAST: { // 最小的一个
            result = ValueNull.INSTANCE;
            for (int i = 0; i < args.length; i++) {
                Value v = getNullOrValue(session, args, values, i);
                if (!(v == ValueNull.INSTANCE)) {
                    v = v.convertTo(dataType);
                    if (result == ValueNull.INSTANCE) {
                        result = v;
                    } else {
                        int comp = database.compareTypeSafe(result, v);
                        if (info.type == GREATEST && comp < 0) {
                            result = v;
                        } else if (info.type == LEAST && comp > 0) {
                            result = v;
                        }
                    }
                }
            }
            break;
        }
        case CASE: {
            Expression then = null;
            // 像这样: SELECT SET(@v, 15), CASE WHEN @v<10 THEN 'Low' ELSE 'High' END
            // 此时v0为null
            if (v0 == null) {
                // Searched CASE expression
                // (null, when, then)
                // (null, when, then, else)
                // (null, when, then, when, then)
                // (null, when, then, when, then, else)
                for (int i = 1, len = args.length - 1; i < len; i += 2) {
                    Value when = args[i].getValue(session);
                    if (when.getBoolean()) {
                        then = args[i + 1];
                        break;
                    }
                }
            } else {
                // Simple CASE expression
                // (expr, when, then)
                // (expr, when, then, else)
                // (expr, when, then, when, then)
                // (expr, when, then, when, then, else)
                if (v0 != ValueNull.INSTANCE) {
                    for (int i = 1, len = args.length - 1; i < len; i += 2) {
                        Value when = args[i].getValue(session);
                        if (session.getDatabase().areEqual(v0, when)) {
                            then = args[i + 1];
                            break;
                        }
                    }
                }
            }
            if (then == null && args.length % 2 == 0) {
                // then = elsePart
                then = args[args.length - 1];
            }
            result = then == null ? ValueNull.INSTANCE : then.getValue(session);
            break;
        }
        case ARRAY_GET: {
            if (v0.getType() == Value.ARRAY) {
                int element = v1.getInt(); // 下标从1开始
                Value[] list = ((ValueArray) v0).getList();
                if (element < 1 || element > list.length) {
                    result = ValueNull.INSTANCE;
                } else {
                    result = list[element - 1];
                }
            } else {
                result = ValueNull.INSTANCE;
            }
            break;
        }
        case ARRAY_CONTAINS: {
            result = ValueBoolean.get(false);
            if (v0.getType() == Value.ARRAY) {
                Value[] list = ((ValueArray) v0).getList();
                if (v1 instanceof ValueArray) {
                    result = ValueBoolean.get(true);
                    Value[] list2 = ((ValueArray) v1).getList();
                    for (int i = 0; i < list2.length; i++) {
                        v1 = list2[i];
                        boolean b = false;
                        for (Value v : list) {
                            if (v.equals(v1)) {
                                b = true;
                                break;
                            }
                        }
                        if (b == false) {
                            result = ValueBoolean.get(false);
                            break;
                        }
                    }
                } else {
                    for (Value v : list) {
                        if (v.equals(v1)) {
                            result = ValueBoolean.get(true);
                            break;
                        }
                    }
                }
            }
            break;
        }
        default:
            throw getUnsupportedException();
        }
        return result;
    }

    private Sequence getSequence(ServerSession session, Value v0, Value v1) {
        String schemaName, sequenceName;
        if (v1 == null) {
            LealoneSQLParser p = (LealoneSQLParser) session.getParser();
            String sql = v0.getString();
            Expression expr = p.parseExpression(sql);
            if (expr instanceof ExpressionColumn) {
                ExpressionColumn seq = (ExpressionColumn) expr;
                schemaName = seq.getOriginalTableAliasName();
                if (schemaName == null) {
                    schemaName = session.getCurrentSchemaName();
                    sequenceName = sql;
                } else {
                    sequenceName = seq.getColumnName();
                }
            } else {
                throw DbException.getSyntaxError(sql, 1);
            }
        } else {
            schemaName = v0.getString();
            sequenceName = v1.getString();
        }
        Schema s = database.findSchema(session, schemaName);
        if (s == null) {
            schemaName = StringUtils.toUpperEnglish(schemaName);
            s = database.getSchema(session, schemaName);
        }
        Sequence seq = s.findSequence(session, sequenceName);
        if (seq == null) {
            sequenceName = StringUtils.toUpperEnglish(sequenceName);
            seq = s.getSequence(session, sequenceName);
        }
        return seq;
    }

    private static void setCsvDelimiterEscape(Csv csv, String fieldSeparator, String fieldDelimiter,
            String escapeCharacter) {
        if (fieldSeparator != null) {
            csv.setFieldSeparatorWrite(fieldSeparator);
            if (fieldSeparator.length() > 0) {
                char fs = fieldSeparator.charAt(0);
                csv.setFieldSeparatorRead(fs);
            }
        }
        if (fieldDelimiter != null) {
            char fd = fieldDelimiter.length() == 0 ? 0 : fieldDelimiter.charAt(0);
            csv.setFieldDelimiter(fd);
        }
        if (escapeCharacter != null) {
            char ec = escapeCharacter.length() == 0 ? 0 : escapeCharacter.charAt(0);
            csv.setEscapeCharacter(ec);
        }
    }

    @Override
    protected void checkParameterCount(int len) {
        int min = 0, max = Integer.MAX_VALUE;
        switch (info.type) {
        case COALESCE:
        case CSVREAD:
        case LEAST:
        case GREATEST:
            min = 1;
            break;
        case FILE_READ:
            min = 1;
            max = 2;
            break;
        case CASE:
        case CSVWRITE:
            min = 2;
            break;
        case CURRVAL:
        case NEXTVAL:
            min = 1;
            max = 2;
            break;
        case DECODE:
            min = 3;
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
        case IFNULL:
        case NULLIF:
        case COALESCE:
        case LEAST:
        case GREATEST:
        case DECODE: {
            t = Value.UNKNOWN;
            s = 0;
            p = 0;
            d = 0;
            int i = 0;
            for (Expression e : args) {
                if (info.type == DECODE) {
                    if (i < 2 || ((i % 2 == 1) && (i != args.length - 1))) {
                        // decode(a, b, whenB)
                        // decode(a, b, whenB, else)
                        // decode(a, b, whenB, c, whenC)
                        // decode(a, b, whenB, c, whenC, end)
                        i++;
                        continue;
                    }
                }
                if (e != ValueExpression.getNull()) {
                    int type = e.getType();
                    if (type != Value.UNKNOWN && type != Value.NULL) {
                        t = Value.getHigherOrder(t, type);
                        s = Math.max(s, e.getScale());
                        p = Math.max(p, e.getPrecision());
                        d = Math.max(d, e.getDisplaySize());
                    }
                }
                i++;
            }
            if (t == Value.UNKNOWN) {
                t = Value.STRING;
                s = 0;
                p = Integer.MAX_VALUE;
                d = Integer.MAX_VALUE;
            }
            break;
        }
        case CASE: {
            t = args[2].getType();
            p = args[2].getPrecision();
            s = args[2].getScale();
            d = args[2].getDisplaySize();
            for (int i = 3, len = args.length - 1; i < len; i += 2) {
                int index = i + 1;
                t = Value.getHigherOrder(t, args[index].getType());
                p = Math.max(p, args[index].getPrecision());
                s = Math.max(s, args[index].getScale());
                d = Math.max(d, args[index].getDisplaySize());
            }
            if (args.length % 2 == 0) { // elsePart
                int index = args.length - 1;
                t = Value.getHigherOrder(t, args[index].getType());
                p = Math.max(p, args[index].getPrecision());
                s = Math.max(s, args[index].getScale());
                d = Math.max(d, args[index].getDisplaySize());
            }
            break;
        }
        case CASEWHEN:
            t = Value.getHigherOrder(args[1].getType(), args[2].getType());
            p = Math.max(args[1].getPrecision(), args[2].getPrecision());
            d = Math.max(args[1].getDisplaySize(), args[2].getDisplaySize());
            s = Math.max(args[1].getScale(), args[2].getScale());
            break;
        case NVL2:
            switch (args[1].getType()) {
            case Value.STRING:
            case Value.CLOB:
            case Value.STRING_FIXED:
            case Value.STRING_IGNORECASE:
                t = args[1].getType();
                break;
            default:
                t = Value.getHigherOrder(args[1].getType(), args[2].getType());
                break;
            }
            p = Math.max(args[1].getPrecision(), args[2].getPrecision());
            d = Math.max(args[1].getDisplaySize(), args[2].getDisplaySize());
            s = Math.max(args[1].getScale(), args[2].getScale());
            break;
        case CAST:
        case CONVERT:
        case TRUNCATE_VALUE:
            // data type, precision and scale is already set
            t = dataType;
            p = precision;
            s = scale;
            d = displaySize;
            break;
        case SET: {
            Expression p1 = args[1];
            t = p1.getType();
            p = p1.getPrecision();
            s = p1.getScale();
            d = p1.getDisplaySize();
            if (!(p0 instanceof Variable)) {
                throw DbException.get(ErrorCode.CAN_ONLY_ASSIGN_TO_VARIABLE_1, p0.getSQL());
            }
            break;
        }
        case FILE_READ: {
            if (args.length == 1) {
                t = Value.BLOB;
            } else {
                t = Value.CLOB;
            }
            p = Integer.MAX_VALUE;
            s = 0;
            d = Integer.MAX_VALUE;
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
            if (v == ValueNull.INSTANCE) {
                if (info.type == CAST || info.type == CONVERT) {
                    return this;
                }
            }
            return ValueExpression.get(v);
        }
        return this;
    }

    @Override
    public ValueResultSet getValueForColumnList(ServerSession session, Expression[] args) {
        if (info.type == CSVREAD) {
            String fileName = args[0].getValue(session).getString();
            if (fileName == null) {
                throw DbException.get(ErrorCode.PARAMETER_NOT_SET_1, "fileName");
            }
            String columnList = args.length < 2 ? null : args[1].getValue(session).getString();
            Csv csv = new Csv();
            String options = args.length < 3 ? null : args[2].getValue(session).getString();
            String charset = null;
            if (options != null && options.indexOf('=') >= 0) {
                charset = csv.setOptions(options);
            } else {
                charset = options;
                String fieldSeparatorRead = args.length < 4 ? null
                        : args[3].getValue(session).getString();
                String fieldDelimiter = args.length < 5 ? null : args[4].getValue(session).getString();
                String escapeCharacter = args.length < 6 ? null : args[5].getValue(session).getString();
                setCsvDelimiterEscape(csv, fieldSeparatorRead, fieldDelimiter, escapeCharacter);
            }
            char fieldSeparator = csv.getFieldSeparatorRead();
            String[] columns = StringUtils.arraySplit(columnList, fieldSeparator);
            ResultSet rs = null;
            ValueResultSet x;
            try {
                rs = csv.read(fileName, columns, charset);
                x = ValueResultSet.getCopy(rs, 0);
            } catch (SQLException e) {
                throw DbException.convert(e);
            } finally {
                JdbcUtils.closeSilently(rs);
            }
            return x;
        }
        return super.getValueForColumnList(session, args);
    }

    @Override
    public String getSQL() {
        StatementBuilder buff = new StatementBuilder(info.name);
        if (info.type == CASE) {
            if (args[0] != null) {
                buff.append(" ").append(args[0].getSQL());
            }
            for (int i = 1, len = args.length - 1; i < len; i += 2) {
                buff.append(" WHEN ").append(args[i].getSQL());
                buff.append(" THEN ").append(args[i + 1].getSQL());
            }
            if (args.length % 2 == 0) {
                buff.append(" ELSE ").append(args[args.length - 1].getSQL());
            }
            return buff.append(" END").toString();
        }
        buff.append('(');
        switch (info.type) {
        case CAST: {
            buff.append(args[0].getSQL()).append(" AS ")
                    .append(new Column(null, dataType, precision, scale, displaySize).getCreateSQL());
            break;
        }
        case CONVERT: {
            buff.append(args[0].getSQL()).append(',')
                    .append(new Column(null, dataType, precision, scale, displaySize).getCreateSQL());
            break;
        }
        default:
            appendArgs(buff);
        }
        return buff.append(')').toString();
    }

    @Override
    boolean isBufferResultSetToLocalTemp() {
        return info.type == CSVREAD;
    }
}
