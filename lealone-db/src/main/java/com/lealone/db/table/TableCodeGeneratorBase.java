/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.db.table;

import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;

public abstract class TableCodeGeneratorBase extends PluginBase implements TableCodeGenerator {

    public TableCodeGeneratorBase(String name) {
        super(name);
    }

    @Override
    public Class<? extends Plugin> getPluginClass() {
        return TableCodeGenerator.class;
    }
}
