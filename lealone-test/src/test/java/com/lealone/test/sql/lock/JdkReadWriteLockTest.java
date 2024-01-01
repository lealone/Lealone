/*
 * Copyright Lealone Database Group.
 * Licensed under the Server Side Public License, v 1.
 * Initial Developer: zhh
 */
package com.lealone.test.sql.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JdkReadWriteLockTest {

    public static void main(String[] args) throws Exception {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        Lock sharedLock = lock.readLock();
        Lock exclusiveLock = lock.writeLock();
        Thread t1 = new Thread(() -> {
            try {
                // 先获得写锁再要读锁是可以的，但是反过来不行
                // 获得写锁后必须释放两次，否则其他线程不能获得写锁
                boolean b = exclusiveLock.tryLock();
                System.out.println(b);
                b = exclusiveLock.tryLock();
                System.out.println(b);
                b = sharedLock.tryLock();
                System.out.println(b);
                exclusiveLock.unlock();
                sharedLock.unlock();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Thread t2 = new Thread(() -> {
            try {
                boolean b = sharedLock.tryLock();
                System.out.println(b);
                b = exclusiveLock.tryLock();
                System.out.println(b);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t1.start();
        t2.start();
        t1.join();
        t2.join();
    }
}
