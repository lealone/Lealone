/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import java.util.List;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.db.table.Column;
import org.lealone.db.value.Value;

public abstract class ServiceExecutorBase implements ServiceExecutor {

    protected Map<String, ServiceMethod> serviceMethodMap;

    protected Object[] getServiceMethodArgs(String methodName, Value[] methodArgs) {
        Object[] args = new Object[methodArgs.length];
        // 如果没有定义服务方法，直接把传递进来的方法参数转成对象
        if (serviceMethodMap.isEmpty()) {
            for (int i = 0; i < methodArgs.length; i++) {
                Value v = methodArgs[i];
                args[i] = getServiceMethodArg(v.getType(), v);
            }
        } else {
            ServiceMethod m = serviceMethodMap.get(methodName);
            List<Column> parameters = m.getParameters();
            for (int i = 0; i < parameters.size(); i++) {
                Column c = parameters.get(i);
                Value v = methodArgs[i];
                args[i] = getServiceMethodArg(c.getType(), v);
            }
        }
        return args;
    }

    protected Object getServiceMethodArg(int type, Value v) {
        Object arg = null;
        switch (type) {
        case Value.BOOLEAN:
            arg = v.getBoolean();
            break;
        case Value.BYTE:
            arg = v.getByte();
            break;
        case Value.SHORT:
            arg = v.getShort();
            break;
        case Value.INT:
            arg = v.getInt();
            break;
        case Value.LONG:
            arg = v.getLong();
            break;
        case Value.DECIMAL:
            arg = v.getBigDecimal();
            break;
        case Value.TIME:
            arg = v.getFloat();
            break;
        case Value.DATE:
            arg = v.getDate();
            break;
        case Value.TIMESTAMP:
            arg = v.getTimestamp();
            break;
        case Value.BYTES:
            arg = v.getBytes();
            break;
        case Value.UUID:
            arg = v.getUuid();
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            arg = v.getString();
            break;
        case Value.BLOB:
            arg = v.getBlob();
            break;
        case Value.CLOB:
            arg = v.getClob();
            break;
        case Value.ARRAY:
            arg = v.getArray();
            break;
        case Value.DOUBLE:
            arg = v.getDouble();
            break;
        case Value.FLOAT:
            arg = v.getFloat();
            break;
        case Value.NULL:
        case Value.JAVA_OBJECT:
        case Value.UNKNOWN:
        case Value.RESULT_SET:
            arg = v.getObject();
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return arg;
    }

    protected Object[] getServiceMethodArgs(String methodName, Map<String, Object> methodArgs) {
        if (serviceMethodMap.isEmpty()) {
            return null;
        }
        ServiceMethod m = serviceMethodMap.get(methodName);
        List<Column> parameters = m.getParameters();
        Object[] args = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            Column c = parameters.get(i);
            String cName = c.getName();
            args[i] = getServiceMethodArg(cName, c.getType(), methodArgs);
        }
        return args;
    }

    protected Object getServiceMethodArg(String cName, int type, Map<String, Object> methodArgs) {
        Object arg = null;
        switch (type) {
        case Value.BOOLEAN:
            arg = toBoolean(cName, methodArgs);
            break;
        case Value.BYTE:
            arg = toByte(cName, methodArgs);
            break;
        case Value.SHORT:
            arg = toShort(cName, methodArgs);
            break;
        case Value.INT:
            arg = toInt(cName, methodArgs);
            break;
        case Value.LONG:
            arg = toLong(cName, methodArgs);
            break;
        case Value.DECIMAL:
            arg = toBigDecimal(cName, methodArgs);
            break;
        case Value.TIME:
            arg = toTime(cName, methodArgs);
            break;
        case Value.DATE:
            arg = toDate(cName, methodArgs);
            break;
        case Value.TIMESTAMP:
            arg = toTimestamp(cName, methodArgs);
            break;
        case Value.BYTES:
            arg = toBytes(cName, methodArgs);
            break;
        case Value.UUID:
            arg = toUUID(cName, methodArgs);
            break;
        case Value.STRING:
        case Value.STRING_IGNORECASE:
        case Value.STRING_FIXED:
            arg = toString(cName, methodArgs);
            break;
        case Value.BLOB:
            arg = toBlob(cName, methodArgs);
            break;
        case Value.CLOB:
            arg = toClob(cName, methodArgs);
            break;
        case Value.ARRAY:
            arg = toArray(cName, methodArgs);
            break;
        case Value.DOUBLE:
            arg = toDouble(cName, methodArgs);
            break;
        case Value.FLOAT:
            arg = toFloat(cName, methodArgs);
            break;
        case Value.NULL:
        case Value.JAVA_OBJECT:
        case Value.UNKNOWN:
        case Value.RESULT_SET:
            arg = toObject(cName, methodArgs);
            break;
        default:
            throw DbException.getInternalError("type=" + type);
        }
        return arg;
    }

    protected Object[] getServiceMethodArgs(String methodName, String json) {
        if (serviceMethodMap.isEmpty()) {
            return null;
        }
        ServiceMethod m = serviceMethodMap.get(methodName);
        List<Column> parameters = m.getParameters();
        Object[] args = new Object[parameters.size()];
        JsonArrayGetter getter = JsonArrayGetter.create(json);
        for (int i = 0; i < parameters.size(); i++) {
            Column c = parameters.get(i);
            args[i] = getter.getValue(i, c.getType());
        }
        return args;
    }
}
