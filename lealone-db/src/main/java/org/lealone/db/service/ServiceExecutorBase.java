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
        ServiceMethod m = serviceMethodMap.get(methodName);
        List<Column> parameters = m.getParameters();
        Object[] args = new Object[methodArgs.length];
        for (int i = 0; i < parameters.size(); i++) {
            Column c = parameters.get(i);
            Object arg = null;
            Value v = methodArgs[i];
            switch (c.getType()) {
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
            case Value.DOUBLE:
                arg = v.getDouble();
                break;
            case Value.FLOAT:
                arg = v.getFloat();
                break;
            case Value.NULL:
                return null;
            case Value.JAVA_OBJECT:
                arg = v.getObject();
                break;
            case Value.UNKNOWN:
                arg = v.getObject();
                break;
            case Value.ARRAY:
                arg = v.getArray();
                break;
            case Value.RESULT_SET:
                arg = v.getObject();
                break;
            default:
                throw DbException.getInternalError("type=" + c.getType());
            }
            args[i] = arg;
        }
        return args;
    }

    protected Object[] getServiceMethodArgs(String methodName, Map<String, Object> methodArgs) {
        ServiceMethod m = serviceMethodMap.get(methodName);
        List<Column> parameters = m.getParameters();
        Object[] args = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            Column c = parameters.get(i);
            String cName = c.getName();
            Object arg = null;
            switch (c.getType()) {
            case Value.BOOLEAN:
                arg = Boolean.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.BYTE:
                arg = Byte.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.SHORT:
                arg = Short.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.INT:
                arg = Integer.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.LONG:
                arg = Long.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.DECIMAL:
                arg = new java.math.BigDecimal(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.TIME:
                arg = java.sql.Time.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.DATE:
                arg = java.sql.Date.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.TIMESTAMP:
                arg = java.sql.Timestamp.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.BYTES:
                arg = ServiceExecutor.toBytes(cName, methodArgs);
                break;
            case Value.UUID:
                arg = java.util.UUID.fromString(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.STRING:
            case Value.STRING_IGNORECASE:
            case Value.STRING_FIXED:
                arg = ServiceExecutor.toString(cName, methodArgs);
                break;
            case Value.BLOB:
                arg = new org.lealone.db.value.ReadonlyBlob(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.CLOB:
                arg = new org.lealone.db.value.ReadonlyClob(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.DOUBLE:
                arg = Double.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.FLOAT:
                arg = Float.valueOf(ServiceExecutor.toString(cName, methodArgs));
                break;
            case Value.NULL:
                return null;
            case Value.JAVA_OBJECT:
                arg = methodArgs.get(cName);
                break;
            case Value.UNKNOWN:
                arg = methodArgs.get(cName);
                break;
            case Value.ARRAY:
                arg = new org.lealone.db.value.ReadonlyArray(methodArgs.get(cName));
                break;
            case Value.RESULT_SET:
                arg = methodArgs.get(cName);
                break;
            default:
                throw DbException.getInternalError("type=" + c.getType());
            }
            args[i] = arg;
        }
        return args;
    }

    protected Object[] getServiceMethodArgs(String methodName, String json) {
        ServiceMethod m = serviceMethodMap.get(methodName);
        List<Column> parameters = m.getParameters();
        Object[] args = new Object[parameters.size()];
        JsonArrayDecoder decoder = JsonArrayDecoder.create(json);
        for (int i = 0; i < parameters.size(); i++) {
            Column c = parameters.get(i);
            args[i] = decoder.getValue(i, c.getType());
        }
        return args;
    }
}
