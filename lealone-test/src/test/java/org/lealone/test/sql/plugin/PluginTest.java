/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.sql.plugin;

import java.util.Map;

import org.junit.Test;
import org.lealone.db.LealoneDatabase;
import org.lealone.db.Plugin;
import org.lealone.test.sql.SqlTestBase;

public class PluginTest extends SqlTestBase implements Plugin {

    public PluginTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void testPlugin() throws Exception {
        executeUpdate("drop plugin if exists " + getName());
        sql = "create plugin if not exists " + getName() //
                + " implement by '" + PluginTest.class.getName() + "'" //
                + " parameters(aaa=false)";
        executeUpdate(sql);

        executeUpdate("start plugin " + getName());
        executeUpdate("stop plugin " + getName());
        executeUpdate("drop plugin " + getName());
    }

    @Override
    public String getName() {
        return PluginTest.class.getSimpleName();
    }

    @Override
    public Map<String, String> getConfig() {
        return null;
    }

    @Override
    public void init(Map<String, String> config) {
        System.out.println(getName() + " init, config: " + config);
    }

    @Override
    public void close() {
        System.out.println(getName() + " close");
    }

    @Override
    public void start() {
        System.out.println(getName() + " start");
    }

    @Override
    public void stop() {
        System.out.println(getName() + " stop");
    }
}
