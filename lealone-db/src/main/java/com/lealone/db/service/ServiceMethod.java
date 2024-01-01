/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.service;

import java.util.List;

import com.lealone.db.table.Column;

public class ServiceMethod {

    private String methodName;
    private List<Column> parameters;
    private Column returnType;

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public List<Column> getParameters() {
        return parameters;
    }

    public void setParameters(List<Column> parameters) {
        this.parameters = parameters;
    }

    public Column getReturnType() {
        return returnType;
    }

    public void setReturnType(Column returnType) {
        this.returnType = returnType;
    }

}
