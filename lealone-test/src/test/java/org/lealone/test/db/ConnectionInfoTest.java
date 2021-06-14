/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.db;

import java.util.Properties;

import org.junit.Test;
import org.lealone.db.ConnectionInfo;
import org.lealone.db.ConnectionSetting;
import org.lealone.test.UnitTestBase;

public class ConnectionInfoTest extends UnitTestBase {
    @Test
    public void run() {
        setEmbedded(true);

        ConnectionInfo ci = new ConnectionInfo(getURL());

        assertTrue(ci.isEmbedded());
        assertTrue(ci.isPersistent());
        assertFalse(ci.isRemote());
        assertTrue(ci.getDatabaseName() != null && ci.getDatabaseName().endsWith(DEFAULT_DB_NAME));
        assertNull(ci.getServers());

        setEmbedded(false);

        ci = new ConnectionInfo(getURL());
        assertFalse(ci.isEmbedded());
        assertFalse(ci.isPersistent()); // TCP类型的URL在Client端建立连接时无法确定是否是Persistent，所以为false
        assertTrue(ci.isRemote());
        assertEquals(DEFAULT_DB_NAME, ci.getDatabaseName());
        assertEquals(getHostAndPort(), ci.getServers());

        try {
            new ConnectionInfo("invalid url");
            fail();
        } catch (Exception e) {
        }

        setMysqlUrlStyle(true);
        try {
            new ConnectionInfo(getURL() + ";a=b"); // MySQL风格的URL中不能出现';'
            fail();
        } catch (Exception e) {
        }

        setMysqlUrlStyle(false);
        try {
            new ConnectionInfo(getURL() + "&a=b"); // 默认风格的URL中不能出现'&'
            fail();
        } catch (Exception e) {
        }

        setEmbedded(true);
        try {
            new ConnectionInfo(getURL(), "mydb"); // 传递到Server端构建ConnectionInfo时URL不会是嵌入式的
            fail();
        } catch (Exception e) {
        }

        try {
            new ConnectionInfo(getURL() + ";a=b"); // a是一个非法的参数名
            fail();
        } catch (Exception e) {
        }

        try {
            String key = ConnectionSetting.IS_LOCAL.name();
            Properties prop = new Properties();
            prop.setProperty(key, "true");
            // url中设置的参数跟用Properties设置的参数虽然重复了，但值是一样的，所以合法
            new ConnectionInfo(getURL() + ";" + key + "=true", prop);
            // 值不一样了，所以是非法的
            new ConnectionInfo(getURL() + ";" + key + "=false", prop);
            fail();
        } catch (Exception e) {
        }
    }
}
