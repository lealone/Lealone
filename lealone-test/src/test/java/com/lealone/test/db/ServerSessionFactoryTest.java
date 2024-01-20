/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db;

import org.junit.Test;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;

public class ServerSessionFactoryTest extends DbTestBase {
    @Test
    public void run() {
        setInMemory(true);
        setEmbedded(false); // 如果是true的话会自动创建数据库

        try {
            createServerSession(getURL("NOT_FOUND"));
            fail();
        } catch (DbException e) {
            assertEquals(ErrorCode.DATABASE_NOT_FOUND_1, e.getErrorCode());
        }
    }
}
