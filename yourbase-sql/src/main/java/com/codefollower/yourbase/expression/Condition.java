/*
 * Copyright 2004-2011 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.codefollower.yourbase.expression;

import com.codefollower.yourbase.value.Value;
import com.codefollower.yourbase.value.ValueBoolean;

/**
 * Represents a condition returning a boolean value, or NULL.
 */
abstract class Condition extends Expression {

    public int getType() {
        return Value.BOOLEAN;
    }

    public int getScale() {
        return 0;
    }

    public long getPrecision() {
        return ValueBoolean.PRECISION;
    }

    public int getDisplaySize() {
        return ValueBoolean.DISPLAY_SIZE;
    }

}
