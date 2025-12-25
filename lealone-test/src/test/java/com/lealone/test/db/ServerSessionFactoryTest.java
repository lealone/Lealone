/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.db;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import com.lealone.common.exceptions.DbException;
import com.lealone.db.api.ErrorCode;
import com.lealone.db.scheduler.EmbeddedScheduler;
import com.lealone.db.scheduler.Scheduler;
import com.lealone.db.scheduler.SchedulerFactory;

public class ServerSessionFactoryTest extends DbTestBase {
    @Test
    public void run() {
        setInMemory(true);
        setEmbedded(false); // 如果是true的话会自动创建数据库

        SchedulerFactory schedulerFactory = SchedulerFactory.getSchedulerFactory(EmbeddedScheduler.class,
                new HashMap<>(), true);
        Scheduler scheduler = schedulerFactory.getScheduler();
        CountDownLatch latch = new CountDownLatch(1);
        scheduler.handle(() -> {
            try {
                createServerSession(getURL("NOT_FOUND"));
                fail();
            } catch (DbException e) {
                assertEquals(ErrorCode.DATABASE_NOT_FOUND_1, e.getErrorCode());
            }
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
