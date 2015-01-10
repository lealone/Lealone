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
package org.lealone.transaction.local;

import java.util.concurrent.atomic.AtomicInteger;

class TimestampService {
    private final AtomicInteger last;

    TimestampService(int lastTransactionId) {
        this.last = new AtomicInteger(lastTransactionId);
    }

    //分布式事务用奇数版本号
    public int nextOdd() {
        int oldLast;
        int last;
        int delta;
        do {
            oldLast = this.last.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 1;
            else
                delta = 2;

            last += delta;
        } while (!this.last.compareAndSet(oldLast, last));
        return last;
    }

    //本地事务用偶数版本号
    public int nextEven() {
        int oldLast;
        int last;
        int delta;
        do {
            oldLast = this.last.get();
            last = oldLast;
            if (last % 2 == 0)
                delta = 2;
            else
                delta = 1;
            last += delta;
        } while (!this.last.compareAndSet(oldLast, last));
        return last;
    }

    @Override
    public String toString() {
        return "TimestampService(last: " + last + ")";
    }
}
