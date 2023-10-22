/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package org.lealone.test.plugins.mysql;

import org.junit.Test;
import org.lealone.common.util.Utils;
import org.lealone.plugins.mysql.server.util.RandomUtil;
import org.lealone.plugins.mysql.server.util.SecurityUtil;

public class PasswordTest extends MySQLTestBase {
    @Test
    public void run() throws Exception {
        String password = "PasswordTest";
        byte[] seed = RandomUtil.randomBytes(20);
        byte[] hash1 = SecurityUtil.scramble411(password.getBytes(), seed);

        byte[] sha1Pass = SecurityUtil.sha1(password);
        byte[] hash2 = SecurityUtil.scramble411Sha1Pass(sha1Pass, seed);

        assertTrue(Utils.compareSecure(hash1, hash2));
    }
}
