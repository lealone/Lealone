/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.codefollower.yourbase.omid.client;

import java.util.concurrent.CountDownLatch;

class SyncCallbackBase implements Callback {
    private Exception e = null;
    private CountDownLatch latch = new CountDownLatch(1);;

    public Exception getException() {
        return e;
    }

    synchronized public void error(Exception e) {
        this.e = e;
        countDown();
    }

    protected void countDown() {
        latch.countDown();
    }

    public void await() throws InterruptedException {
        latch.await();
    }
}