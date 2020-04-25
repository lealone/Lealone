/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.test.misc.lock;

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
