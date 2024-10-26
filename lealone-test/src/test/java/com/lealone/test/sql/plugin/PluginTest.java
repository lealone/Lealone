/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.plugin;

import java.util.Map;

import org.junit.Test;

import com.lealone.db.LealoneDatabase;
import com.lealone.db.plugin.Plugin;
import com.lealone.db.plugin.PluginBase;
import com.lealone.test.sql.SqlTestBase;

public class PluginTest extends SqlTestBase {

    public PluginTest() {
        super(LealoneDatabase.NAME);
    }

    @Test
    public void testPlugin() throws Exception {
        executeUpdate("drop plugin if exists " + getPluginName());
        sql = "create plugin if not exists " + getPluginName() //
                + " implement by '" + MyPlugin.class.getName() + "'" //
                + " parameters(aaa=false)";
        executeUpdate(sql);

        executeUpdate("start plugin " + getPluginName());
        executeUpdate("stop plugin " + getPluginName());
        executeUpdate("drop plugin " + getPluginName());
    }

    public static String getPluginName() {
        return MyPlugin.class.getSimpleName();
    }

    public static class MyPlugin extends PluginBase {

        public MyPlugin() {
            super(getPluginName());
        }

        @Override
        public Class<? extends Plugin> getPluginClass() {
            return Plugin.class;
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
}
