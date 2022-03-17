/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.sql.operator;

import org.lealone.db.PluginBase;

public abstract class OperatorFactoryBase extends PluginBase implements OperatorFactory {

    public OperatorFactoryBase(String name) {
        super(name);
    }
}
