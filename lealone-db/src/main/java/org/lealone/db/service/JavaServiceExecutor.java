/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.db.service;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.lealone.common.exceptions.DbException;
import org.lealone.common.util.CamelCaseHelper;
import org.lealone.db.value.Value;
import org.lealone.db.value.ValueNull;
import org.lealone.db.value.ValueString;

public class JavaServiceExecutor extends ServiceExecutorBase {

    private final Service service;
    private Map<String, Method> objectMethodMap;
    private Object implementClassObject;

    public JavaServiceExecutor(Service service) {
        this.service = service;
    }

    // 第一次调用时再初始化，否则会影响启动时间
    private void init() {
        if (implementClassObject != null)
            return;
        Class<?> implementClass;
        try {
            implementClass = Class.forName(service.getImplementBy());
            implementClassObject = implementClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("newInstance exception: " + service.getImplementBy(), e);
        }
        int size = service.getServiceMethods().size();
        serviceMethodMap = new HashMap<>(size);
        objectMethodMap = new HashMap<>(size);
        if (size <= 0) {
            for (Method m : implementClass.getDeclaredMethods()) {
                int modifiers = m.getModifiers();
                if (Modifier.isPublic(modifiers)) {
                    objectMethodMap.put(m.getName().toUpperCase(), m);
                }
            }
        } else {
            for (ServiceMethod serviceMethod : service.getServiceMethods()) {
                String serviceMethodName = serviceMethod.getMethodName();
                serviceMethodMap.put(serviceMethodName, serviceMethod);

                String objectMethodName = CamelCaseHelper.toCamelFromUnderscore(serviceMethodName);
                try {
                    // 不使用getDeclaredMethod，因为这里不考虑参数，只要方法名匹配即可
                    for (Method m : implementClass.getDeclaredMethods()) {
                        if (m.getName().equals(objectMethodName)) {
                            objectMethodMap.put(serviceMethodName, m);
                            break;
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException("Method not found: " + objectMethodName, e);
                }
            }
        }
    }

    @Override
    public Value executeService(String methodName, Value[] methodArgs) {
        init();
        Object[] args = getServiceMethodArgs(methodName, methodArgs);
        Method method = objectMethodMap.get(methodName);
        try {
            Object ret = method.invoke(implementClassObject, args);
            if (ret == null)
                return ValueNull.INSTANCE;
            return ValueString.get(ret.toString());
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String executeService(String methodName, Map<String, Object> methodArgs) {
        init();
        Object[] args = getServiceMethodArgs(methodName, methodArgs);
        Method method = objectMethodMap.get(methodName);
        try {
            Object ret = method.invoke(implementClassObject, args);
            if (ret == null)
                return null;
            return ret.toString();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public String executeService(String methodName, String json) {
        init();
        Object[] args = getServiceMethodArgs(methodName, json);
        Method method = objectMethodMap.get(methodName);
        try {
            Object ret = method.invoke(implementClassObject, args);
            if (ret == null)
                return null;
            return ret.toString();
        } catch (Exception e) {
            throw DbException.convert(e);
        }
    }
}
